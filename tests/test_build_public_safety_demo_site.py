from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


PACKAGE_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "package_public_safety_demo.py"
sys.path.insert(0, str(PACKAGE_SCRIPT_PATH.parent))
PACKAGE_SPEC = importlib.util.spec_from_file_location("package_public_safety_demo_script", PACKAGE_SCRIPT_PATH)
assert PACKAGE_SPEC and PACKAGE_SPEC.loader
PACKAGE_MODULE = importlib.util.module_from_spec(PACKAGE_SPEC)
sys.modules[PACKAGE_SPEC.name] = PACKAGE_MODULE
PACKAGE_SPEC.loader.exec_module(PACKAGE_MODULE)

SITE_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "build_public_safety_demo_site.py"
SITE_SPEC = importlib.util.spec_from_file_location("build_public_safety_demo_site_script", SITE_SCRIPT_PATH)
assert SITE_SPEC and SITE_SPEC.loader
SITE_MODULE = importlib.util.module_from_spec(SITE_SPEC)
sys.modules[SITE_SPEC.name] = SITE_MODULE
SITE_SPEC.loader.exec_module(SITE_MODULE)


def test_build_public_safety_demo_site_extracts_bundle_and_writes_shell(tmp_path: Path) -> None:
    bundle_path = PACKAGE_MODULE.package_public_safety_demo(
        output_dir=tmp_path / "bundle-output",
        profile="small",
        seed=42,
        formats=("csv", "parquet"),
        version="0.6.0",
    )

    site_dir = SITE_MODULE.build_public_safety_demo_site(
        bundle_path=bundle_path,
        output_dir=tmp_path / "site-output",
        site_title="Hosted Demo Shell",
    )

    assert (site_dir / "index.html").exists()
    assert (site_dir / "site_manifest.json").exists()
    assert (site_dir / "bundle" / "demo_manifest.json").exists()
    assert (site_dir / "bundle" / "data" / "public_safety_demo" / "public_safety_demo_dashboard.html").exists()

    index_text = (site_dir / "index.html").read_text(encoding="utf-8")
    assert "<title>Hosted Demo Shell</title>" in index_text
    assert "Embedded Demo Dashboard" in index_text
    assert "Suggested Demo Scenarios" in index_text
    assert 'iframe src="bundle/data/public_safety_demo/public_safety_demo_dashboard.html"' in index_text

    site_manifest = json.loads((site_dir / "site_manifest.json").read_text(encoding="utf-8"))
    assert site_manifest == {
        "bundle_name": bundle_path.name,
        "bundle_root": "bundle",
        "index": "index.html",
        "site_title": "Hosted Demo Shell",
    }
