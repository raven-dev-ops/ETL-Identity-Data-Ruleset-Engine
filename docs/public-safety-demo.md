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

Package the stronger standalone customer pilot handoff with seeded
state, a prepared demo shell, and startup helpers:

```bash
python scripts/package_customer_pilot_bundle.py --output-dir dist/customer-pilot
```

Prepare a standalone Django + SQLite demo shell from that bundle:

```bash
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --profile small --seed 42 --formats csv,parquet --prepare-only
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --profile small --seed 42 --formats csv,parquet --host 127.0.0.1 --port 8000
```

Or load the same shell directly from a persisted run:

```bash
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --state-db data/state/pipeline_state.sqlite --run-id RUN-20260314T000000Z-EXAMPLE --prepare-only
python scripts/run_public_safety_demo_shell.py --output-dir dist/public-safety-demo-django --state-db data/state/pipeline_state.sqlite --run-id RUN-20260314T000000Z-EXAMPLE --host 127.0.0.1 --port 8000
```

That standalone shell uses Django's default SQLite backend and serves a
read-only local walkthrough over either the extracted demo bundle or a
materialized persisted run. It is the recommended demonstration path
when you want the project to stay self-contained and walk an ID
Network-style buyer through CAD calls, RMS reports, and master-person
resolution in one local app.

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

The customer-pilot packaging path goes further and also includes the
seed dataset, the persisted SQLite run state, a prepared demo-shell
workspace, a minimal runtime payload, and one-command launch helpers.
See [customer-pilot-bundle.md](customer-pilot-bundle.md).
For the supported Windows-first single-host PostgreSQL pilot bootstrap,
see [windows-pilot-bootstrap.md](windows-pilot-bootstrap.md).

The Django shell preparation script writes a standalone local workspace
under `dist/public-safety-demo-django/` by default:

- `db.sqlite3`
- `bundle/` with the extracted or materialized demo artifacts
- the original demo zip when bundle mode is used
- `persisted_run_source.json` when persisted-state mode is used

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

## Canonical Regression Scenarios

The demo and onboarding story now has a checked-in regression set in
`fixtures/public_safety_regressions/`.

Use those scenarios when someone asks for the sharp edge cases behind
the dashboard:

- `Same Person Across CAD And RMS`
  - expected result: one golden person across both systems
- `Same Household, Different People`
  - expected result: separate golden people despite shared address and
    surname
- `Cross-System False Merge Guard`
  - expected result: no merge for a soundalike pair with the same DOB

That fixture tree is the canonical proof set for the current public-
safety onboarding path.

## Read-Model Contract

The demo shell and the authenticated service now share the same stable
public-safety read model:

- `golden_person_activity`
  - one row per resolved master person with CAD/RMS activity counts
  - field set matches
    `data/public_safety_demo/golden_person_activity.csv`
- `incident_identity_view`
  - one row per incident/person/source join mapped back to the resolved
    golden person
  - field set matches
    `data/public_safety_demo/incident_identity_view.csv`

Bundle mode, persisted-state mode, and the documented service endpoints
all consume those same field sets. That keeps buyer walkthroughs,
offline demo bundles, and read-only integrations aligned.

## Scope Boundary

This layer is a mock-data demonstration surface only.

- It is safe for synthetic demos.
- It is not a claim of real CAD or RMS system integration.
- It is not, by itself, a CJIS compliance control set.

Use it to demonstrate the identity-resolution pattern and the downstream
read model, not to imply production public-safety deployment readiness
without the separate security, hosting, and policy controls.
