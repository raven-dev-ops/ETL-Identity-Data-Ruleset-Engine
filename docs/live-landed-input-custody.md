# Live Landed-Input Custody

Synthetic fixtures in `fixtures/` and the sample rows emitted by
`prepare-live-target-pack` are for rehearsal, tests, and onboarding
walkthroughs only. They are not the supported custody workflow for live
customer exports.

For live landed inputs, use this path:

1. Render the supported target pack.
2. Replace the sample rows with the landed customer extracts while
   preserving filenames and header shapes.
3. Validate the staged pack.
4. Seal the staged pack into an immutable landing directory with a
   custody manifest.

Example:

```bash
etl-identity-engine prepare-live-target-pack --target-id cad_county_dispatch_v1 --output-dir D:/etl/live-targets/cad_county_dispatch_v1 --set agency_name="Franklin County Dispatch"
etl-identity-engine check-live-target-pack --target-id cad_county_dispatch_v1 --root-dir D:/etl/live-targets/cad_county_dispatch_v1
etl-identity-engine capture-live-target-custody --target-id cad_county_dispatch_v1 --staged-root D:/etl/live-targets/cad_county_dispatch_v1 --output-dir D:/etl/landed-batches --operator-id dispatch.operator --transport-channel sftp --tenant-id tenant-a
```

## Custody Manifest

`capture-live-target-custody` writes a timestamped immutable directory
under the selected `--output-dir`. That directory contains:

- the copied `batch_manifest.yml`
- the copied target-pack bundle directory and landing files
- the rendered `README.md`
- the rendered `live_target_pack_summary.json`
- `custody_manifest.json`

`custody_manifest.json` records:

- `target_id`
- `source_class`
- `vendor_profile`
- `operator_id`
- `transport_channel`
- `arrived_at_utc`
- `captured_at_utc`
- `immutable_root`
- `tracked_files`
- `replay_linkage`

Each `tracked_files` entry records:

- original filename
- original staged path
- immutable copied path
- file size
- `sha256`
- file role such as `batch_manifest`, `landing_source`, or
  `source_bundle_file`

`replay_linkage` records the batch manifest path, batch id, resolved
input paths, target id, and source-bundle ids needed to connect the
sealed landing set back to a replayable manifest-driven run.

## Scope Boundary

This workflow is the supported live-input path from customer drop zone
to validated manifest input. Keep the resulting immutable landing
directories outside the repo and do not commit live customer exports or
custody manifests.

If you need a shareable onboarding artifact after capture, generate a
masked acceptance package from the immutable landing set:

```bash
etl-identity-engine package-live-target-acceptance --target-id cad_county_dispatch_v1 --source-root D:/etl/landed-batches/20260316T040300Z-cad_county_dispatch_v1 --output-dir D:/etl/acceptance-packages
```

That workflow is documented in `docs/live-acceptance-packages.md`.
