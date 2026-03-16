# Live Target Packs

The repo now ships maintained live onboarding target packs for the first
supported CAD and RMS export shapes:

- `cad_county_dispatch_v1`
- `rms_records_bureau_v1`

Each target pack packages four things together:

- a concrete batch manifest scaffold
- a bundle-local `contract_manifest.yml` pinned to the supported
  `vendor_profile`
- synthetic sample landing and vendor-bundle files that already pass the
  current onboarding checks
- a rendered `README.md` plus `live_target_pack_summary.json` carrying
  the resolved customer variables for review

List the shipped packs:

```bash
etl-identity-engine list-live-target-packs
```

## CAD Target

Render the supported CAD pack:

```bash
etl-identity-engine prepare-live-target-pack --target-id cad_county_dispatch_v1 --output-dir dist/live-targets/cad_county_dispatch_v1 --set agency_name="Franklin County Dispatch" --set agency_slug=franklin-county-dispatch --set drop_zone_subpath=cad/franklin_dispatch/inbound --set operator_contact=dispatch.integration@example.gov
etl-identity-engine check-live-target-pack --target-id cad_county_dispatch_v1 --root-dir dist/live-targets/cad_county_dispatch_v1
```

Rendered customer variables:

- `agency_name`
  - rendered into `README.md` and `live_target_pack_summary.json`
- `agency_slug`
  - rendered into `batch_manifest.yml` as the sample batch id prefix
- `drop_zone_subpath`
  - rendered into `README.md` as the expected customer-managed inbound
    drop-zone location before local staging
- `operator_contact`
  - rendered into `README.md` for onboarding ownership

Concrete CAD landing contract:

- staged manifest path: `batch_manifest.yml`
- staged bundle directory: `cad_county_dispatch_bundle/`
- fixed bundle filenames:
  - `vendor_person_records.csv`
  - `vendor_incident_records.csv`
  - `vendor_incident_person_links.csv`
- fixed contract marker:
  - `cad_county_dispatch_bundle/contract_manifest.yml`
  - `vendor_profile: cad_county_dispatch_v1`

Replace the sample rows with landed customer extracts while preserving
the filenames and header shapes. No ad hoc manifest editing should be
required during normal onboarding for this target.

For live landed batches, seal the staged pack into an immutable custody
directory before running the pipeline:

```bash
etl-identity-engine capture-live-target-custody --target-id cad_county_dispatch_v1 --staged-root dist/live-targets/cad_county_dispatch_v1 --output-dir D:/etl/landed-batches --operator-id dispatch.operator --transport-channel sftp
```

## RMS Target

Render the supported RMS pack:

```bash
etl-identity-engine prepare-live-target-pack --target-id rms_records_bureau_v1 --output-dir dist/live-targets/rms_records_bureau_v1 --set agency_name="Franklin County Records Bureau" --set agency_slug=franklin-county-records-bureau --set drop_zone_subpath=rms/franklin_records/inbound --set operator_contact=records.integration@example.gov
etl-identity-engine check-live-target-pack --target-id rms_records_bureau_v1 --root-dir dist/live-targets/rms_records_bureau_v1
```

Rendered customer variables follow the same contract as the CAD target.

Concrete RMS landing contract:

- staged manifest path: `batch_manifest.yml`
- staged bundle directory: `rms_records_bureau_bundle/`
- fixed bundle filenames:
  - `vendor_person_records.csv`
  - `vendor_incident_records.csv`
  - `vendor_incident_person_links.csv`
- fixed contract marker:
  - `rms_records_bureau_bundle/contract_manifest.yml`
  - `vendor_profile: rms_records_bureau_v1`

Replace the sample rows with landed customer extracts while preserving
the filenames and header shapes. No ad hoc manifest editing should be
required during normal onboarding for this target.

For live landed batches, seal the staged pack into an immutable custody
directory before running the pipeline:

```bash
etl-identity-engine capture-live-target-custody --target-id rms_records_bureau_v1 --staged-root dist/live-targets/rms_records_bureau_v1 --output-dir D:/etl/landed-batches --operator-id records.operator --transport-channel smb_share
```

See `docs/live-landed-input-custody.md` for the custody manifest and
immutable landing workflow.

When you need a reviewable package after live capture, use
`package-live-target-acceptance` to emit masked fixture files plus drift
reports without carrying raw landed values or custody metadata into the
package. That workflow is documented in `docs/live-acceptance-packages.md`.
