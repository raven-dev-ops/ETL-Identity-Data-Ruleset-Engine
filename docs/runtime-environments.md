# Runtime Environments

The runtime now supports named environment profiles so operators can
switch between development, test, and production defaults without
editing committed YAML files in place.

The default catalog now also includes a `container` environment for the
single-host deployment baseline shipped in
[container-deployment.md](container-deployment.md), a `cluster`
environment for the PostgreSQL-backed Kubernetes baseline in
[kubernetes-deployment.md](kubernetes-deployment.md), a `cluster_ha`
environment for the external-HA PostgreSQL app baseline in
[kubernetes-ha-deployment.md](kubernetes-ha-deployment.md), and a `cjis`
environment for the stricter JWT + PostgreSQL deployment baseline
documented in [cjis-deployment-baseline.md](cjis-deployment-baseline.md).

## Environment File

The default environment catalog is:

- `config/runtime_environments.yml`

It defines:

- `default_environment`
- `environments.<name>.config_dir`
- `environments.<name>.state_db`
- `environments.<name>.secrets`
- `environments.<name>.service_auth`
- `environments.<name>.field_authorization`

Relative paths are resolved from the directory that contains the runtime
environment file. `environments.<name>.state_db` supports either a
local SQLite path or a PostgreSQL SQLAlchemy URL.

## Variable Resolution

Both runtime environment files and per-environment YAML overlays support
`${ENV_VAR}` and `${ENV_VAR:-default}` placeholders.

- `${ENV_VAR}` requires the variable to be present at runtime.
- `${ENV_VAR:-default}` falls back to `default` when the variable is not
  set.
- For any placeholder-backed value, the runtime also supports a mounted
  secret-file companion variable named `${ENV_VAR}_FILE`. When
  `ENV_VAR` is unset and `ENV_VAR_FILE` points to a readable file, the
  runtime reads the file contents, trims trailing whitespace, and uses
  that value instead.

This is the supported mechanism for secret-backed values in the current
line. Secrets should not be committed directly into the repo config.

## Service Auth Settings

Runtime environments can also define service authentication defaults
under:

- `environments.<name>.service_auth.mode`
- `environments.<name>.service_auth.header_name`
- `environments.<name>.service_auth.reader_api_key`
- `environments.<name>.service_auth.operator_api_key`
- `environments.<name>.service_auth.issuer`
- `environments.<name>.service_auth.audience`
- `environments.<name>.service_auth.algorithms`
- `environments.<name>.service_auth.jwt_secret`
- `environments.<name>.service_auth.jwt_public_key_pem`
- `environments.<name>.service_auth.role_claim`
- `environments.<name>.service_auth.scope_claim`
- `environments.<name>.service_auth.reader_roles`
- `environments.<name>.service_auth.operator_roles`
- `environments.<name>.service_auth.reader_scopes`
- `environments.<name>.service_auth.operator_scopes`
- `environments.<name>.service_auth.subject_claim`

Two modes are supported:

- `mode: jwt`
  - validates bearer tokens against configured issuer, audience,
    algorithms, and deployment-provided signing material
  - maps external claims into internal `reader` and `operator` roles
  - `role_claim` and `subject_claim` may use dotted paths such as
    `realm_access.roles`
  - `scope_claim` may also use a dotted path and can narrow the token's
    effective permission set below the default role scope set
- `mode: api_key`
  - keeps the simpler static API-key compatibility path

The stable service scope names for the current line are:

- `service:health`
- `service:metrics`
- `runs:read`
- `runs:replay`
- `runs:publish`
- `golden:read`
- `crosswalk:read`
- `public_safety:read`
- `review_cases:read`
- `review_cases:write`
- `exports:run`

The default production environment now uses JWT bearer auth with:

- `ETL_IDENTITY_SERVICE_JWT_ISSUER`
- `ETL_IDENTITY_SERVICE_JWT_AUDIENCE`
- `ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM`

Mounted-secret deployments should prefer the `_FILE` companions instead:

- `ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM_FILE`
- `ETL_IDENTITY_SERVICE_READER_API_KEY_FILE`
- `ETL_IDENTITY_SERVICE_OPERATOR_API_KEY_FILE`
- `ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY_FILE`
- `ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY_FILE`

If `mode: api_key` is configured and both API-key values resolve to
blank strings, service auth is treated as unconfigured. `serve-api`
then fails fast until the deployment environment provides both values.

For startup and periodic rotation checks, use:

```bash
python -m etl_identity_engine.cli check-runtime-auth-material \
  --environment prod \
  --max-secret-file-age-hours 720
```

`serve-api` accepts the same `--max-secret-file-age-hours` option and
also honors `ETL_IDENTITY_RUNTIME_AUTH_MAX_AGE_HOURS` when you want the
rotation-age gate to come from the deployment environment.

The `cjis` environment is stricter than `prod` in one important way: it
pins JWT validation to `RS256`, requires deployment-supplied object
storage secrets, and is intended to be paired with the dedicated CJIS
preflight rather than a looser general production check.

The `container` environment is different from `prod` in one important
way: it provides default placeholder object-storage secret values so the
local container topology can start without cloud credentials. Service
authentication there remains in API-key compatibility mode.

The `cluster` environment follows the same API-key compatibility model,
but it requires a deployment-supplied PostgreSQL URL through
`ETL_IDENTITY_STATE_DB` and is intended for the shipped Kubernetes
topology.

The `cluster_ha` environment follows the same API-key compatibility
model, but it is intended for the external-HA PostgreSQL app baseline.
That line expects a stable writer endpoint such as
`identity-postgres-rw` and relies on the database platform to promote a
new primary behind that writer DNS or service name.

## Field Authorization Settings

Runtime environments may optionally define field-level authorization
rules under `environments.<name>.field_authorization`.

If the block is omitted, the runtime allows all documented fields on the
supported read and delivery surfaces.

Each configured surface maps stable field names to one of:

- `allow`
- `mask`
- `deny`

Supported service read surfaces:

- `service.golden_record`
- `service.crosswalk_lookup`
- `service.public_safety_golden_activity`
- `service.public_safety_incident_identity`

Supported delivery and export surfaces:

- `delivery.golden_records`
- `delivery.source_to_golden_crosswalk`

Example:

```yaml
environments:
  prod:
    field_authorization:
      service.golden_record:
        first_name: mask
        phone: mask
      delivery.golden_records:
        phone: deny
```

Behavior is intentionally narrow and fail-closed:

- `mask` preserves the documented response or CSV shape and replaces
  non-empty string values with `[MASKED]`
- `deny` blocks the entire request or publication job for that surface
- invalid surface names, field names, or actions fail runtime-config
  loading
- unexpected evaluation failures return errors instead of silently
  emitting partially filtered data

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
