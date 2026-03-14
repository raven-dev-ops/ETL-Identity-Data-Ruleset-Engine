# Runtime Environments

The runtime now supports named environment profiles so operators can
switch between development, test, and production defaults without
editing committed YAML files in place.

The default catalog now also includes a `container` environment for the
single-host deployment baseline shipped in
[container-deployment.md](container-deployment.md).

## Environment File

The default environment catalog is:

- `config/runtime_environments.yml`

It defines:

- `default_environment`
- `environments.<name>.config_dir`
- `environments.<name>.state_db`
- `environments.<name>.secrets`
- `environments.<name>.service_auth`

Relative paths are resolved from the directory that contains the runtime
environment file.

## Variable Resolution

Both runtime environment files and per-environment YAML overlays support
`${ENV_VAR}` and `${ENV_VAR:-default}` placeholders.

- `${ENV_VAR}` requires the variable to be present at runtime.
- `${ENV_VAR:-default}` falls back to `default` when the variable is not
  set.

This is the supported mechanism for secret-backed values in the current
line. Secrets should not be committed directly into the repo config.

## Service Auth Settings

Runtime environments can also define service authentication defaults
under:

- `environments.<name>.service_auth.header_name`
- `environments.<name>.service_auth.reader_api_key`
- `environments.<name>.service_auth.operator_api_key`

The default production environment uses:

- `ETL_IDENTITY_SERVICE_READER_API_KEY`
- `ETL_IDENTITY_SERVICE_OPERATOR_API_KEY`

If both API-key values resolve to blank strings, service auth is treated
as unconfigured. `serve-api` then fails fast until the deployment
environment provides both values.

The `container` environment is different from `prod` in one important
way: it provides default placeholder object-storage secret values so the
local container topology can start without cloud credentials. Service
API keys still must be supplied for `serve-api`.

## Config Overlays

Pipeline rule files still live under `config/`, and named environments
can override any subset of them under:

- `config/environments/<environment>/normalization_rules.yml`
- `config/environments/<environment>/blocking_rules.yml`
- `config/environments/<environment>/matching_rules.yml`
- `config/environments/<environment>/thresholds.yml`
- `config/environments/<environment>/survivorship_rules.yml`

Overlay mappings are merged on top of the base repo config before
validation runs.

## CLI Usage

Use a named environment for rule-loading commands:

```bash
python -m etl_identity_engine.cli match \
  --input data/normalized/normalized_person_records.csv \
  --output data/matches/candidate_scores.csv \
  --environment prod
```

Use a named environment for persisted-state commands:

```bash
python -m etl_identity_engine.cli state-db-upgrade \
  --environment dev
```

You can also point at a non-default environment file:

```bash
python -m etl_identity_engine.cli run-all \
  --base-dir . \
  --environment prod \
  --runtime-config deploy/runtime_environments.yml
```
