# Public Safety Demo

This repo now includes a concrete synthetic public-safety demo layer for
mock CAD and RMS activity.

The goal is simple: show how incident activity from two operational
systems can be tied back to one resolved master person identity without
using real CJIS data. The standalone shell is tuned for an ID
Network-style buyer walkthrough, not for claiming a live integration.

## What It Uses

The shipped synthetic generator already writes:

- `data/synthetic_sources/incident_records.*`
- `data/synthetic_sources/incident_person_links.*`
- `data/golden/golden_person_records.csv`
- `data/golden/source_to_golden_crosswalk.csv`

The `public-safety-demo` stage joins those artifacts into a
demonstration read model.

## Commands

Generate the full mock pipeline plus the demo artifacts:

```bash
python -m etl_identity_engine.cli run-all --base-dir demo-output --profile small --seed 42
```

Rebuild only the demo slice from an existing synthetic run:

```bash
python -m etl_identity_engine.cli public-safety-demo --base-dir demo-output
```

Package one deterministic demo bundle for handoff:

```bash
python scripts/package_public_safety_demo.py --output-dir dist/public-safety-demo --profile small --seed 42 --formats csv,parquet
```

Prepare a standalone Django + SQLite demo shell from that bundle:

```bash
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --profile small --seed 42 --formats csv,parquet --prepare-only
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --profile small --seed 42 --formats csv,parquet --host 127.0.0.1 --port 8000
```

That standalone shell uses Django's default SQLite backend and serves a
read-only local walkthrough over the extracted demo bundle. It is the
recommended demonstration path when you want the project to stay
self-contained and walk an ID Network-style buyer through CAD calls,
RMS reports, and master-person resolution in one local app.

Optional fallback: build a hostable static site shell from that same
bundle:

```bash
python scripts/build_public_safety_demo_site.py --bundle dist/public-safety-demo/etl-identity-engine-v<version>-public-safety-demo-small.zip --output-dir dist/public-safety-demo-site --site-title "Hosted Public Safety Identity Demo"
```

## Output Files

The stage writes:

- `data/public_safety_demo/incident_identity_view.csv`
- `data/public_safety_demo/golden_person_activity.csv`
- `data/public_safety_demo/public_safety_demo_dashboard.html`
- `data/public_safety_demo/public_safety_demo_report.md`
- `data/public_safety_demo/public_safety_demo_scenarios.json`
- `data/public_safety_demo/public_safety_demo_summary.json`
- `data/public_safety_demo/public_safety_demo_walkthrough.md`

The packaging script writes a zip bundle that includes the dashboard,
the joined demo artifacts, the golden outputs, and the synthetic CAD/RMS
incident files used to build the demo.
It now also includes the recommended scenario list and a generated live
demo walkthrough.

The Django shell preparation script writes a standalone local workspace
under `dist/public-safety-demo-django/` by default:

- `db.sqlite3`
- `bundle/` with the extracted demo artifacts and original zip

That shell serves:

- an overview page with summary cards and scenario links
- scenario detail pages
- master-person detail pages
- direct raw artifact downloads

The static-site builder extracts that bundle into a hostable directory
with:

- `index.html`
- `site_manifest.json`
- `bundle/` with the extracted demo artifacts

That output can be uploaded directly to a static host such as GitHub
Pages, Netlify, Azure Static Web Apps, S3 static hosting, or Cloudflare
Pages.

## What It Demonstrates

`incident_identity_view.csv` shows:

- which incident came from `cad` or `rms`
- which source person record was linked to that incident
- which golden person the record resolved to
- the surviving golden name, DOB, address, and phone

`golden_person_activity.csv` shows:

- incident counts per golden person
- whether that person appears in CAD, RMS, or both
- the latest incident timestamp
- the set of observed participant roles

This is the fastest concrete story for a demo:

1. Generate the synthetic run.
2. Run `scripts/run_public_safety_demo_shell.py`.
3. Open the standalone Django shell in the browser.
4. Pick a person with both CAD and RMS activity.
5. Use the recent incident view for that `golden_id`.
6. Show that multiple operational incident records roll up to one
   resolved identity.

## Scope Boundary

This layer is a mock-data demonstration surface only.

- It is safe for synthetic demos.
- It is not a claim of real CAD or RMS system integration.
- It is not, by itself, a CJIS compliance control set.

Use it to demonstrate the identity-resolution pattern and the downstream
read model, not to imply production public-safety deployment readiness
without the separate security, hosting, and policy controls.
