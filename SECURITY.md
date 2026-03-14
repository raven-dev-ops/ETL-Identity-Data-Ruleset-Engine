# Security Policy

## Scope

This repository is a synthetic-data prototype and must not contain:

- credentials, API keys, or tokens
- production exports
- personally identifying operational records

## Reporting

If you find a security issue, open a private report through the repository security advisory flow:

- <https://github.com/raven-dev-ops/ETL-Identity-Data-Ruleset-Engine/security/advisories/new>

If private reporting is unavailable, open an issue with minimal details and request a secure channel.

## Handling Guidance

- Never commit secrets to source control.
- Rotate exposed credentials immediately.
- Remove sensitive content and force-rotate any impacted keys.

For the supported production deployment boundary, including operator
responsibilities, audit expectations, and rollback ownership, see
[docs/production-operating-model.md](docs/production-operating-model.md).
