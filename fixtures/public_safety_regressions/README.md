# Public Safety Regression Fixtures

This fixture tree packages three canonical onboarding scenarios for the
public-safety path:

- `same_person_cross_system`
  - one person appears in both CAD and RMS and must merge
- `same_household_separate_people`
  - two people share a household footprint and must stay separate
- `cross_system_false_merge_guard`
  - a soundalike cross-system pair shares DOB but must not merge

Use it with:

```bash
etl-identity-engine check-public-safety-onboarding --manifest fixtures/public_safety_regressions/manifest.yml
python -m etl_identity_engine.cli run-all --base-dir dist/public-safety-regressions --manifest fixtures/public_safety_regressions/manifest.yml
```

The fixture is intended to keep onboarding and demo behavior grounded in
explicit merge and no-merge outcomes.
