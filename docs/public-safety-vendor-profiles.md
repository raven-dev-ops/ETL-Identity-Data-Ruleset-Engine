# Public Safety Vendor Profiles

The runtime now ships maintained vendor-profile overlays for common
public-safety onboarding shapes. These profiles let operators validate
vendor-native CAD and RMS extracts without first rewriting the source columns
into the canonical contract headers.

Current shipped profiles:

- `cad_county_dispatch_v1`
  - contract: `cad_call_for_service`
  - source class: `cad`
  - shape: county dispatch export with separate person, event, and link
    keys
- `cad_records_management_v1`
  - contract: `cad_call_for_service`
  - source class: `cad`
  - shape: records-oriented CAD export with call, subject, and person
    identifiers
- `rms_case_management_v1`
  - contract: `rms_report_person`
  - source class: `rms`
  - shape: case-management RMS export with report, subject, and link
    identifiers
- `rms_records_bureau_v1`
  - contract: `rms_report_person`
  - source class: `rms`
  - shape: records-bureau RMS export with report, party, and
    master-person identifiers

These profiles are packaged with the installed Python distribution and
work in local, object-storage, and installed-wheel environments. They
are part of the supported onboarding surface for the current release
line.

## Where Vendor Profiles Apply

You can apply a packaged vendor profile in three places:

1. Bundle-local marker:

```yaml
contract_name: rms_report_person
contract_version: v1
vendor_profile: rms_case_management_v1
files:
  person_records: vendor_person_records.csv
  incident_records: vendor_incident_records.csv
  incident_person_links: vendor_incident_person_links.csv
```

2. Manifest-declared source bundle:

```yaml
source_bundles:
  - bundle_id: rms_primary
    source_class: rms
    path: rms_bundle
    contract_name: rms_report_person
    contract_version: v1
    vendor_profile: rms_case_management_v1
```

3. Direct contract validation:

```bash
etl-identity-engine validate-public-safety-contract --bundle-dir ./rms_bundle --vendor-profile rms_case_management_v1
```

## Rules

- `vendor_profile` and `mapping_overlay` are mutually exclusive for the
  same source bundle or contract marker.
- The selected profile must match the declared contract name and
  version.
- The profile still enforces the canonical CAD or RMS contract after
  mapping, including required files, required fields, `source_system`,
  and link integrity.

## Current Scope

The current packaged profile set covers both CAD and RMS onboarding.
