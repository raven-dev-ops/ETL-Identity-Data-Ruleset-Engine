# Standards Mapping

This document maps the current repo-side deployment baseline to the
external controls it is trying to support. It is not a certification.

## CJIS Sources

- CJIS Security Policy v5.9.5, dated July 9, 2024:
  <https://le.fbi.gov/file-repository/cjis-security-policy-v5_9_5-20240709.pdf>
- CJIS Security Addendum:
  <https://le.fbi.gov/cjis-division/cjis-security-policy-resource-center/appendicies/security-addendum.pdf/view>

## Repo-Side Mapping

| Control Area | Repo Baseline | Primary Repo Surface |
| --- | --- | --- |
| Strong access control | JWT-based service auth for `cjis` runtime, no API-key compatibility path | `config/runtime_environments.yml`, `docs/runtime-environments.md` |
| Protected transport and host material | Required TLS certificate and key paths in the preflight | `deploy/cjis.env.example`, `scripts/cjis_preflight_check.py` |
| Audit logging | Required audit-log directory plus structured logs and persisted audit events with shared free-text/auth redaction | `docs/operations-observability.md`, `scripts/cjis_preflight_check.py` |
| Encrypted protected storage | Required PostgreSQL state store and object-storage secret material | `config/runtime_environments.yml`, `scripts/cjis_preflight_check.py` |
| Backup and recovery | Required backup root plus replay/restore runbooks | `docs/recovery-runbooks.md`, `scripts/cjis_preflight_check.py` |
| MFA and personnel attestations | Required affirmative deployment attestations in the preflight | `deploy/cjis.env.example`, `scripts/cjis_preflight_check.py` |
| Contractor / operator governance | Explicit operator boundary and Security Addendum acknowledgement flag | `SECURITY.md`, `deploy/cjis.env.example`, `scripts/cjis_preflight_check.py` |

## Scope Boundary

The repo can only validate the controls it can see:

- runtime configuration
- auth mode and JWT wiring
- deployment-supplied file paths
- deployment-supplied attestations
- logging, backup, and recovery surfaces

The repo cannot itself certify:

- agency approval
- personnel screening outcomes
- signed Security Addendum execution
- physical security
- cloud enclave accreditation
- policy/process implementation quality

Use this mapping to show what the product can support operationally, not
to claim automatic CJIS compliance.
