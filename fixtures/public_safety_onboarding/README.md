# Public Safety Onboarding Fixtures

These fixtures are a contract-valid local onboarding example for the
`cad_call_for_service` and `rms_report_person` bundle contracts plus a
matching production batch manifest.

Files included:

- `example_manifest.yml`
- `landing/source_a.csv`
- `landing/source_b.csv`
- `cad_bundle/`
- `rms_bundle/`

Use them with:

```bash
etl-identity-engine check-public-safety-onboarding --manifest fixtures/public_safety_onboarding/example_manifest.yml
```

Or validate the CAD and RMS bundles directly:

```bash
etl-identity-engine check-public-safety-onboarding \
  --bundle-dir fixtures/public_safety_onboarding/cad_bundle \
  --bundle-dir fixtures/public_safety_onboarding/rms_bundle
```

The installed runtime also ships packaged CAD vendor profiles that can
be applied during validation without a bundle-local overlay file:

- `cad_county_dispatch_v1`
- `cad_records_management_v1`

It also now ships packaged RMS vendor profiles:

- `rms_case_management_v1`
- `rms_records_bureau_v1`
