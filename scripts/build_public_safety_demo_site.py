from __future__ import annotations

import argparse
import json
from html import escape
from pathlib import Path
from typing import Sequence
import shutil
import zipfile


DEFAULT_OUTPUT_DIR = Path("dist") / "public-safety-demo-site"
MANIFEST_NAME = "demo_manifest.json"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a hostable static site shell around a public-safety demo bundle."
    )
    parser.add_argument(
        "--bundle",
        required=True,
        help="Path to the public-safety demo zip bundle.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the static site should be written.",
    )
    parser.add_argument(
        "--site-title",
        default="Public Safety Identity Demo",
        help="Title to render in the generated hosted site shell.",
    )
    return parser.parse_args(argv)


def resolve_output_dir(output_dir: str) -> Path:
    path = Path(output_dir)
    if path.is_absolute():
        return path
    return Path.cwd() / path


def _load_json(path: Path) -> dict[str, object] | list[object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _table_html(headers: list[str], rows: list[list[str]]) -> str:
    header_html = "".join(f"<th>{escape(header)}</th>" for header in headers)
    body_html = "".join(
        "<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    if not body_html:
        body_html = '<tr><td colspan="99">No rows</td></tr>'
    return (
        '<div class="table-wrap"><table>'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{body_html}</tbody>"
        "</table></div>"
    )


def _scenario_cards_html(scenarios: list[dict[str, object]]) -> str:
    cards = []
    for scenario in scenarios:
        cards.append(
            '<article class="scenario-card">'
            f'<p class="scenario-kicker">{escape(str(scenario.get("scenario_id", "")))}</p>'
            f'<h3>{escape(str(scenario.get("title", "")))}</h3>'
            f'<p class="scenario-person">{escape(str(scenario.get("golden_name", "")))} '
            f'<span>{escape(str(scenario.get("golden_id", "")))}</span></p>'
            '<div class="scenario-metrics">'
            f'<span>CAD {escape(str(scenario.get("cad_incident_count", 0)))}</span>'
            f'<span>RMS {escape(str(scenario.get("rms_incident_count", 0)))}</span>'
            f'<span>Total {escape(str(scenario.get("total_incident_count", 0)))}</span>'
            "</div>"
            f'<p class="scenario-copy">{escape(str(scenario.get("narrative", "")))}</p>'
            "</article>"
        )
    return "".join(cards) or '<article class="scenario-card"><p>No scenarios available.</p></article>'


def build_site_html(
    *,
    site_title: str,
    manifest: dict[str, object],
    summary: dict[str, object],
    scenarios: list[dict[str, object]],
    golden_activity_rows: list[dict[str, str]],
) -> str:
    top_rows = sorted(
        golden_activity_rows,
        key=lambda row: (
            -int(row.get("total_incident_count", "0")),
            row.get("golden_last_name", ""),
            row.get("golden_first_name", ""),
        ),
    )[:10]
    top_table = _table_html(
        ["Golden ID", "Name", "CAD", "RMS", "Total", "Roles", "Latest Incident"],
        [
            [
                row.get("golden_id", ""),
                " ".join(
                    part
                    for part in (row.get("golden_first_name", "").strip(), row.get("golden_last_name", "").strip())
                    if part
                ),
                row.get("cad_incident_count", "0"),
                row.get("rms_incident_count", "0"),
                row.get("total_incident_count", "0"),
                row.get("roles", ""),
                row.get("latest_incident_at", ""),
            ]
            for row in top_rows
        ],
    )
    version = escape(str(manifest.get("version", "")))
    profile = escape(str(manifest.get("profile", "")))
    seed = escape(str(manifest.get("seed", "")))
    generated_at = escape(str(manifest.get("generated_at_utc", "")))
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(site_title)}</title>
  <style>
    :root {{
      --ink: #10233d;
      --ink-soft: #48607b;
      --paper: #f4ede1;
      --card: rgba(255,255,255,0.94);
      --line: rgba(16,35,61,0.12);
      --blue: #165ba8;
      --red: #ad2e24;
      --green: #146b4d;
      --amber: #9b5a00;
      --teal: #0f766e;
      --shadow: 0 20px 45px rgba(16,35,61,0.12);
      --radius: 22px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Aptos", "Segoe UI Variable Text", "Trebuchet MS", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(22,91,168,0.16), transparent 22rem),
        radial-gradient(circle at bottom right, rgba(173,46,36,0.12), transparent 20rem),
        linear-gradient(160deg, #faf7f2 0%, var(--paper) 60%, #eee2ce 100%);
    }}
    main {{
      width: min(1240px, calc(100vw - 2rem));
      margin: 0 auto;
      padding: 1.4rem 0 3rem;
      display: grid;
      gap: 1rem;
    }}
    .shell, .panel {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }}
    .shell {{
      padding: 1.6rem;
      display: grid;
      gap: 1rem;
    }}
    .eyebrow {{
      display: inline-flex;
      width: fit-content;
      padding: 0.35rem 0.75rem;
      border-radius: 999px;
      background: rgba(16,35,61,0.06);
      letter-spacing: 0.08em;
      text-transform: uppercase;
      font-size: 0.76rem;
      font-weight: 700;
    }}
    h1, h2, h3, p {{ margin: 0; }}
    h1 {{
      font-family: Georgia, "Times New Roman", serif;
      font-size: clamp(2.3rem, 5vw, 4.2rem);
      line-height: 0.94;
      max-width: 14ch;
    }}
    .intro {{
      max-width: 72ch;
      color: var(--ink-soft);
      line-height: 1.55;
      font-size: 1.03rem;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem;
      color: var(--ink-soft);
      font-size: 0.9rem;
    }}
    .meta span {{
      padding: 0.45rem 0.7rem;
      border-radius: 999px;
      background: rgba(16,35,61,0.05);
      border: 1px solid rgba(16,35,61,0.08);
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 0.85rem;
    }}
    .metric {{
      padding: 1rem;
      border-radius: 18px;
      background: rgba(255,255,255,0.85);
      border: 1px solid rgba(16,35,61,0.08);
    }}
    .metric p:first-child {{
      font-size: 0.78rem;
      color: var(--ink-soft);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 0.35rem;
      font-weight: 700;
    }}
    .metric strong {{
      font-size: 2rem;
      line-height: 1;
      display: block;
    }}
    .grid {{
      display: grid;
      gap: 1rem;
      grid-template-columns: 1.1fr 0.9fr;
    }}
    .panel {{
      padding: 1.1rem;
      display: grid;
      gap: 0.9rem;
    }}
    .panel p {{
      color: var(--ink-soft);
      line-height: 1.48;
    }}
    .scenario-grid {{
      display: grid;
      gap: 0.8rem;
    }}
    .scenario-card {{
      padding: 1rem;
      border-radius: 18px;
      background: rgba(16,35,61,0.04);
      border: 1px solid rgba(16,35,61,0.08);
      display: grid;
      gap: 0.45rem;
    }}
    .scenario-kicker {{
      font-size: 0.73rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--ink-soft);
      font-weight: 700;
    }}
    .scenario-person {{
      color: var(--ink-soft);
      font-weight: 600;
    }}
    .scenario-person span {{
      margin-left: 0.45rem;
      font-family: Consolas, monospace;
      font-size: 0.86rem;
    }}
    .scenario-metrics {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.45rem;
      font-size: 0.86rem;
      color: var(--ink-soft);
    }}
    .scenario-metrics span {{
      padding: 0.3rem 0.55rem;
      border-radius: 999px;
      background: rgba(255,255,255,0.78);
      border: 1px solid rgba(16,35,61,0.08);
    }}
    .artifact-links {{
      display: grid;
      gap: 0.55rem;
    }}
    .artifact-links a {{
      color: var(--blue);
      text-decoration: none;
      font-weight: 600;
    }}
    .artifact-links a:hover {{
      text-decoration: underline;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 720px;
    }}
    th, td {{
      padding: 0.72rem 0.75rem;
      border-bottom: 1px solid var(--line);
      text-align: left;
      font-size: 0.94rem;
    }}
    th {{
      font-size: 0.76rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--ink-soft);
    }}
    .embed {{
      border: 1px solid var(--line);
      border-radius: 18px;
      overflow: hidden;
      min-height: 760px;
      background: white;
    }}
    .embed iframe {{
      width: 100%;
      min-height: 760px;
      border: 0;
      display: block;
    }}
    @media (max-width: 920px) {{
      .grid {{
        grid-template-columns: 1fr;
      }}
    }}
    @media (max-width: 720px) {{
      main {{
        width: min(100vw - 1rem, 1240px);
        padding-top: 1rem;
      }}
      .shell, .panel {{
        border-radius: 16px;
      }}
      .shell {{
        padding: 1.2rem;
      }}
      .panel {{
        padding: 0.95rem;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="shell">
      <div class="eyebrow">Hosted Static Demo Shell</div>
      <div>
        <h1>{escape(site_title)}</h1>
        <p class="intro">
          This hostable shell wraps the synthetic CAD/RMS demo bundle with a buyer-facing
          overview, preselected walkthrough scenarios, and direct links to the underlying
          artifacts. It is safe for mock-data demonstrations and does not imply live-system
          integration or CJIS compliance by itself.
        </p>
      </div>
      <div class="meta">
        <span>Version {version}</span>
        <span>Profile {profile}</span>
        <span>Seed {seed}</span>
        <span>Generated {generated_at}</span>
      </div>
      <section class="metrics">
        <article class="metric"><p>Incidents</p><strong>{escape(str(summary.get("incident_count", 0)))}</strong></article>
        <article class="metric"><p>CAD Incidents</p><strong>{escape(str(summary.get("cad_incident_count", 0)))}</strong></article>
        <article class="metric"><p>RMS Incidents</p><strong>{escape(str(summary.get("rms_incident_count", 0)))}</strong></article>
        <article class="metric"><p>Resolved Links</p><strong>{escape(str(summary.get("resolved_link_count", 0)))}</strong></article>
        <article class="metric"><p>Linked Golden People</p><strong>{escape(str(summary.get("linked_golden_person_count", 0)))}</strong></article>
        <article class="metric"><p>Cross-System People</p><strong>{escape(str(summary.get("cross_system_golden_person_count", 0)))}</strong></article>
      </section>
    </section>

    <section class="grid">
      <section class="panel">
        <div>
          <h2>Suggested Demo Scenarios</h2>
          <p>Use these first during the conversation. They are selected automatically from the bundle.</p>
        </div>
        <div class="scenario-grid">{_scenario_cards_html(scenarios)}</div>
      </section>
      <section class="panel">
        <div>
          <h2>Bundle Artifacts</h2>
          <p>Everything here is static and hostable. The original demo dashboard is embedded below.</p>
        </div>
        <nav class="artifact-links">
          <a href="bundle/data/public_safety_demo/public_safety_demo_dashboard.html">Open raw dashboard</a>
          <a href="bundle/data/public_safety_demo/public_safety_demo_walkthrough.md">Open walkthrough</a>
          <a href="bundle/data/public_safety_demo/public_safety_demo_scenarios.json">Open scenarios JSON</a>
          <a href="bundle/data/public_safety_demo/incident_identity_view.csv">Download incident identity view</a>
          <a href="bundle/data/public_safety_demo/golden_person_activity.csv">Download golden activity rollup</a>
          <a href="bundle/demo_manifest.json">Open bundle manifest</a>
        </nav>
      </section>
    </section>

    <section class="panel">
      <div>
        <h2>Top Golden People By Activity</h2>
        <p>Use this table if the audience wants a quick list before you switch to the embedded dashboard.</p>
      </div>
      {top_table}
    </section>

    <section class="panel">
      <div>
        <h2>Embedded Demo Dashboard</h2>
        <p>The hosted shell preserves the original generated dashboard so the bundle can be shown directly from static hosting.</p>
      </div>
      <div class="embed">
        <iframe src="bundle/data/public_safety_demo/public_safety_demo_dashboard.html" title="Embedded public safety demo dashboard"></iframe>
      </div>
    </section>
  </main>
</body>
</html>
"""


def build_public_safety_demo_site(
    *,
    bundle_path: Path,
    output_dir: Path,
    site_title: str,
) -> Path:
    if not bundle_path.exists():
        raise FileNotFoundError(f"Demo bundle not found: {bundle_path}")

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle_output_dir = output_dir / "bundle"
    bundle_output_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(bundle_path) as archive:
        archive.extractall(bundle_output_dir)

    manifest_path = bundle_output_dir / MANIFEST_NAME
    manifest = _load_json(manifest_path)
    if not isinstance(manifest, dict):
        raise ValueError("Demo bundle manifest is not a JSON object")

    summary = _load_json(bundle_output_dir / "data" / "public_safety_demo" / "public_safety_demo_summary.json")
    scenarios = _load_json(bundle_output_dir / "data" / "public_safety_demo" / "public_safety_demo_scenarios.json")
    golden_activity_csv = bundle_output_dir / "data" / "public_safety_demo" / "golden_person_activity.csv"
    import csv
    with golden_activity_csv.open("r", encoding="utf-8", newline="") as handle:
        golden_activity_rows = list(csv.DictReader(handle))

    if not isinstance(summary, dict):
        raise ValueError("Demo summary must be a JSON object")
    if not isinstance(scenarios, list):
        raise ValueError("Demo scenarios must be a JSON array")

    index_path = output_dir / "index.html"
    index_path.write_text(
        build_site_html(
            site_title=site_title,
            manifest=manifest,
            summary=summary,
            scenarios=[item for item in scenarios if isinstance(item, dict)],
            golden_activity_rows=golden_activity_rows,
        ),
        encoding="utf-8",
    )

    site_manifest = {
        "site_title": site_title,
        "bundle_name": bundle_path.name,
        "bundle_root": "bundle",
        "index": "index.html",
    }
    (output_dir / "site_manifest.json").write_text(
        json.dumps(site_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_dir


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = resolve_output_dir(args.output_dir)
    site_dir = build_public_safety_demo_site(
        bundle_path=Path(args.bundle),
        output_dir=output_dir,
        site_title=args.site_title,
    )
    print(f"public safety demo site written: {site_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
