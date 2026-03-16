import csv
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.field_authorization import (
    DELIVERY_GOLDEN_RECORDS_SURFACE,
    SERVICE_GOLDEN_RECORD_SURFACE,
)
from etl_identity_engine.runtime_config import (
    ConfigValidationError,
    evaluate_runtime_auth_material,
    load_benchmark_fixture_configs,
    load_pipeline_config,
    load_runtime_environment,
)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.strip() + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_valid_config(config_dir: Path, *, thresholds: str | None = None) -> None:
    _write_text(
        config_dir / "normalization_rules.yml",
        """
name_normalization:
  trim_whitespace: true
  remove_punctuation: false
  uppercase: false
date_normalization:
  accepted_formats:
    - "%Y-%m-%d"
  output_format: "%Y/%m/%d"
phone_normalization:
  digits_only: true
  output_format: digits_only
  default_country_code: "1"
""",
    )
    _write_text(
        config_dir / "blocking_rules.yml",
        """
blocking_passes:
  - name: birth_year_only
    fields:
      - birth_year
""",
    )
    _write_text(
        config_dir / "matching_rules.yml",
        """
weights:
  canonical_name: 0.5
  canonical_dob: 0.3
  canonical_phone: 0.1
  canonical_address: 0.1
""",
    )
    _write_text(
        config_dir / "thresholds.yml",
        thresholds
        or """
thresholds:
  auto_merge: 0.95
  manual_review_min: 0.5
  no_match_max: 0.49
""",
    )
    _write_text(
        config_dir / "survivorship_rules.yml",
        """
source_priority:
  - source_b
  - source_a
field_rules:
  first_name:
    strategy: source_priority_then_non_null
  last_name:
    strategy: source_priority_then_non_null
  dob:
    strategy: source_priority_then_non_null
  address:
    strategy: source_priority_then_non_null
  phone:
    strategy: source_priority_then_non_null
""",
    )


def _write_runtime_environment_file(path: Path, content: str | None = None) -> None:
    _write_text(
        path,
        content
        or """
default_environment: dev
environments:
  dev:
    config_dir: ./config
    state_db: ./state/dev.sqlite
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    secrets:
      object_storage_access_key: ${TEST_OBJECT_STORAGE_ACCESS_KEY}
    service_auth:
      header_name: X-API-Key
      reader_api_key: ${TEST_SERVICE_READER_API_KEY:-}
      operator_api_key: ${TEST_SERVICE_OPERATOR_API_KEY:-}
      reader_tenant_id: ${TEST_SERVICE_READER_TENANT_ID:-default}
      operator_tenant_id: ${TEST_SERVICE_OPERATOR_TENANT_ID:-default}
""",
    )


def _generate_rsa_public_key_pem() -> str:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")


def test_load_pipeline_config_reads_custom_directory(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)

    config = load_pipeline_config(config_dir)

    assert config.normalization.name.remove_punctuation is False
    assert config.normalization.name.uppercase is False
    assert config.normalization.date.output_format == "%Y/%m/%d"
    assert config.normalization.phone.output_format == "digits_only"
    assert config.normalization.phone.default_country_code == "1"
    assert [blocking_pass.fields for blocking_pass in config.matching.blocking_passes] == [("birth_year",)]
    assert config.matching.weights == {
        "canonical_name": 0.5,
        "canonical_dob": 0.3,
        "canonical_phone": 0.1,
        "canonical_address": 0.1,
    }
    assert config.survivorship.source_priority == ("source_b", "source_a")


def test_cli_commands_respect_custom_config_dir(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_text(
        config_dir / "normalization_rules.yml",
        """
name_normalization:
  trim_whitespace: true
  remove_punctuation: true
  uppercase: true
date_normalization:
  accepted_formats:
    - "%Y-%m-%d"
  output_format: "%Y-%m-%d"
phone_normalization:
  digits_only: true
  output_format: digits_only
  default_country_code: "1"
""",
    )
    _write_text(
        config_dir / "blocking_rules.yml",
        """
blocking_passes:
  - name: last_name_birth_year
    fields:
      - last_name
      - birth_year
""",
    )
    _write_text(
        config_dir / "matching_rules.yml",
        """
weights:
  canonical_name: 1.0
  canonical_dob: 0.0
  canonical_phone: 0.0
  canonical_address: 0.0
""",
    )
    _write_text(
        config_dir / "thresholds.yml",
        """
thresholds:
  auto_merge: 0.95
  manual_review_min: 0.5
  no_match_max: 0.49
""",
    )
    _write_text(
        config_dir / "survivorship_rules.yml",
        """
source_priority:
  - source_b
  - source_a
field_rules:
  first_name:
    strategy: source_priority_then_non_null
  last_name:
    strategy: source_priority_then_non_null
  dob:
    strategy: source_priority_then_non_null
  address:
    strategy: source_priority_then_non_null
  phone:
    strategy: source_priority_then_non_null
""",
    )

    normalized_input = tmp_path / "normalized.csv"
    _write_csv(
        normalized_input,
        [
            {
                "source_record_id": "A-1",
                "person_entity_id": "P-1",
                "source_system": "source_a",
                "first_name": "JOHN",
                "last_name": "SMITH",
                "dob": "1985-03-12",
                "address": "123 MAIN ST",
                "phone": "5551234567",
                "canonical_name": "JOHN SMITH",
                "canonical_dob": "1985-03-12",
                "canonical_address": "123 MAIN ST",
                "canonical_phone": "5551234567",
            },
            {
                "source_record_id": "B-1",
                "person_entity_id": "P-1",
                "source_system": "source_b",
                "first_name": "JONATHAN",
                "last_name": "SMITH",
                "dob": "1985-04-12",
                "address": "123 MAIN STREET",
                "phone": "5551230000",
                "canonical_name": "JOHN SMITH",
                "canonical_dob": "1985-04-12",
                "canonical_address": "123 MAIN STREET",
                "canonical_phone": "5551230000",
            },
        ],
    )

    match_output = tmp_path / "candidate_scores.csv"
    cluster_input = tmp_path / "entity_clusters.csv"
    golden_output = tmp_path / "golden.csv"
    _write_csv(
        cluster_input,
        [
            {
                "cluster_id": "C-00001",
                "source_record_id": "A-1",
                "source_system": "source_a",
                "person_entity_id": "P-1",
            },
            {
                "cluster_id": "C-00001",
                "source_record_id": "B-1",
                "source_system": "source_b",
                "person_entity_id": "P-1",
            },
        ],
    )

    assert (
        main(
            [
                "match",
                "--input",
                str(normalized_input),
                "--output",
                str(match_output),
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "golden",
                "--input",
                str(normalized_input),
                "--clusters",
                str(cluster_input),
                "--output",
                str(golden_output),
                "--config-dir",
                str(config_dir),
            ]
        )
        == 0
    )

    match_rows = _read_csv(match_output)
    golden_rows = _read_csv(golden_output)

    assert len(match_rows) == 1
    assert match_rows[0]["score"] == "1.0"
    assert match_rows[0]["decision"] == "auto_merge"
    assert len(golden_rows) == 1
    assert golden_rows[0]["first_name"] == "JONATHAN"


def test_match_cli_requires_existing_input_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"Input file not found"):
        main(
            [
                "match",
                "--input",
                str(tmp_path / "missing.csv"),
                "--output",
                str(tmp_path / "candidate_scores.csv"),
            ]
        )


def test_golden_cli_requires_cluster_assignments_for_normalized_input(tmp_path: Path) -> None:
    normalized_input = tmp_path / "normalized.csv"
    _write_csv(
        normalized_input,
        [
            {
                "source_record_id": "A-1",
                "person_entity_id": "P-1",
                "source_system": "source_a",
                "first_name": "JOHN",
                "last_name": "SMITH",
                "dob": "1985-03-12",
                "address": "123 MAIN ST",
                "phone": "5551234567",
                "canonical_name": "JOHN SMITH",
                "canonical_dob": "1985-03-12",
                "canonical_address": "123 MAIN ST",
                "canonical_phone": "5551234567",
            }
        ],
    )

    with pytest.raises(FileNotFoundError, match=r"entity_clusters\.csv"):
        main(
            [
                "golden",
                "--input",
                str(normalized_input),
                "--output",
                str(tmp_path / "golden.csv"),
            ]
        )


def test_report_cli_requires_downstream_artifacts(tmp_path: Path) -> None:
    normalized_input = tmp_path / "normalized.csv"
    _write_csv(
        normalized_input,
        [
            {
                "source_record_id": "A-1",
                "person_entity_id": "P-1",
                "source_system": "source_a",
                "first_name": "JOHN",
                "last_name": "SMITH",
                "dob": "1985-03-12",
                "address": "123 MAIN ST",
                "phone": "5551234567",
                "canonical_name": "JOHN SMITH",
                "canonical_dob": "1985-03-12",
                "canonical_address": "123 MAIN ST",
                "canonical_phone": "5551234567",
            }
        ],
    )

    with pytest.raises(FileNotFoundError, match=r"candidate_scores\.csv"):
        main(
            [
                "report",
                "--input",
                str(normalized_input),
                "--output",
                str(tmp_path / "run_report.md"),
            ]
        )


def test_load_pipeline_config_rejects_missing_required_sections(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "thresholds.yml",
        """
not_thresholds:
  auto_merge: 0.95
""",
    )

    with pytest.raises(ValueError, match=r"thresholds\.yml: top-level config contains unsupported keys: not_thresholds"):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_unsupported_blocking_fields(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "blocking_rules.yml",
        """
blocking_passes:
  - name: bad_block
    fields:
      - nickname
""",
    )

    with pytest.raises(ValueError, match=r"blocking_rules\.yml: blocking_passes\[0\]\.fields contains unsupported values: nickname"):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_inconsistent_thresholds(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(
        config_dir,
        thresholds="""
thresholds:
  auto_merge: 1.1
  manual_review_min: 0.7
  no_match_max: 0.7
""",
    )

    with pytest.raises(
        ValueError,
        match=r"thresholds\.yml: thresholds\.no_match_max must be less than thresholds\.manual_review_min",
    ):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_auto_merge_threshold_above_total_weight(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(
        config_dir,
        thresholds="""
thresholds:
  auto_merge: 1.1
  manual_review_min: 0.7
  no_match_max: 0.6
""",
    )

    with pytest.raises(
        ValueError,
        match=r"thresholds\.yml: thresholds\.auto_merge cannot exceed the total configured match weight",
    ):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_missing_weight_fields(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "matching_rules.yml",
        """
weights:
  canonical_name: 1.0
  canonical_dob: 0.0
  canonical_phone: 0.0
""",
    )

    with pytest.raises(
        ValueError,
        match=r"matching_rules\.yml: weights is missing required fields: canonical_address",
    ):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_invalid_survivorship_rules(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "survivorship_rules.yml",
        """
source_priority:
  - source_a
  - source_a
field_rules:
  first_name:
    strategy: source_priority_then_non_null
  last_name:
    strategy: source_priority_then_non_null
  dob:
    strategy: source_priority_then_non_null
  address:
    strategy: source_priority_then_non_null
  phone:
    strategy: newest_value
""",
    )

    with pytest.raises(ValueError, match=r"survivorship_rules\.yml: source_priority contains duplicate source names"):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_invalid_phone_output_format(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "normalization_rules.yml",
        """
name_normalization:
  trim_whitespace: true
  remove_punctuation: false
  uppercase: false
date_normalization:
  accepted_formats:
    - "%Y-%m-%d"
  output_format: "%Y/%m/%d"
phone_normalization:
  digits_only: true
  output_format: international
""",
    )

    with pytest.raises(
        ValueError,
        match=r"normalization_rules\.yml: phone_normalization\.output_format must be one of: digits_only, e164",
    ):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_rejects_unsupported_survivorship_strategy(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "survivorship_rules.yml",
        """
source_priority:
  - source_a
  - source_b
field_rules:
  first_name:
    strategy: source_priority_then_non_null
  last_name:
    strategy: source_priority_then_non_null
  dob:
    strategy: source_priority_then_non_null
  address:
    strategy: source_priority_then_non_null
  phone:
    strategy: newest_value
""",
    )

    with pytest.raises(
        ValueError,
        match=r"survivorship_rules\.yml: field_rules\.phone\.strategy must be one of: source_priority_then_non_null",
    ):
        load_pipeline_config(config_dir)


def test_load_pipeline_config_merges_environment_overlay(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "environments" / "prod" / "thresholds.yml",
        """
thresholds:
  auto_merge: 0.88
  manual_review_min: 0.58
  no_match_max: 0.57
""",
    )
    _write_text(
        config_dir / "environments" / "prod" / "normalization_rules.yml",
        """
phone_normalization:
  output_format: ${TEST_PHONE_OUTPUT_FORMAT:-e164}
""",
    )

    config = load_pipeline_config(config_dir, environment="prod")

    assert config.matching.thresholds.auto_merge == 0.88
    assert config.matching.thresholds.manual_review_min == 0.58
    assert config.normalization.phone.output_format == "e164"


def test_load_runtime_environment_resolves_paths_and_secrets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_environment_file(runtime_config)
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_OBJECT_STORAGE_ACCESS_KEY", "access-key")
    monkeypatch.setenv("TEST_SERVICE_READER_API_KEY", "reader-key")
    monkeypatch.setenv("TEST_SERVICE_OPERATOR_API_KEY", "operator-key")

    environment = load_runtime_environment("prod", runtime_config)

    assert environment.name == "prod"
    assert environment.config_dir == (tmp_path / "config").resolve()
    assert environment.state_db == (tmp_path / "state" / "prod.sqlite").resolve()
    assert environment.secrets == {"object_storage_access_key": "access-key"}
    assert environment.service_auth is not None
    assert environment.service_auth.mode == "api_key"
    assert environment.service_auth.header_name == "X-API-Key"
    assert environment.service_auth.reader_api_key == "reader-key"
    assert environment.service_auth.operator_api_key == "operator-key"
    assert environment.service_auth.reader_tenant_id == "default"
    assert environment.service_auth.operator_tenant_id == "default"
    assert environment.service_auth.reader_scopes == (
        "service:health",
        "service:metrics",
        "runs:read",
        "golden:read",
        "crosswalk:read",
        "public_safety:read",
        "review_cases:read",
    )
    assert environment.service_auth.operator_scopes == (
        "service:health",
        "service:metrics",
        "runs:read",
        "golden:read",
        "crosswalk:read",
        "public_safety:read",
        "review_cases:read",
        "audit_events:read",
        "runs:replay",
        "runs:publish",
        "review_cases:write",
        "exports:run",
    )


def test_load_runtime_environment_uses_supplied_environ_mapping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: dev
environments:
  dev:
    config_dir: ./dev-config
    state_db: ./state/dev.sqlite
  prod:
    config_dir: ./prod-config
    state_db: ${TEST_STATE_DB}
""",
    )
    monkeypatch.setenv("ETL_IDENTITY_ENV", "dev")
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "wrong.sqlite"))

    environment = load_runtime_environment(
        runtime_config_path=runtime_config,
        environ={
            "ETL_IDENTITY_ENV": "prod",
            "TEST_STATE_DB": str(tmp_path / "state" / "prod.sqlite"),
        },
    )

    assert environment.name == "prod"
    assert environment.config_dir == (tmp_path / "prod-config").resolve()
    assert environment.state_db == (tmp_path / "state" / "prod.sqlite").resolve()


def test_load_runtime_environment_supports_optional_tenant_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_environment_file(
        runtime_config,
        """
default_environment: dev
environments:
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    tenant_id: county-a
""",
    )
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))

    environment = load_runtime_environment("prod", runtime_config)

    assert environment.tenant_id == "county-a"
    assert environment.state_db == (tmp_path / "state" / "prod.sqlite").resolve()


def test_load_runtime_environment_requires_declared_secret_env_vars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_environment_file(runtime_config)
    monkeypatch.delenv("TEST_STATE_DB", raising=False)
    monkeypatch.delenv("TEST_OBJECT_STORAGE_ACCESS_KEY", raising=False)

    with pytest.raises(
        ValueError,
        match=r"runtime_environments\.yml: environments\.prod references required environment variable TEST_STATE_DB",
    ):
        load_runtime_environment("prod", runtime_config)


def test_load_runtime_environment_allows_blank_service_auth_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_environment_file(runtime_config)
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_OBJECT_STORAGE_ACCESS_KEY", "access-key")
    monkeypatch.delenv("TEST_SERVICE_READER_API_KEY", raising=False)
    monkeypatch.delenv("TEST_SERVICE_OPERATOR_API_KEY", raising=False)

    environment = load_runtime_environment("prod", runtime_config)

    assert environment.service_auth is None


def test_load_runtime_environment_container_defaults_resolve_without_cloud_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: dev
environments:
  container:
    config_dir: .
    state_db: ${ETL_IDENTITY_STATE_DB:-/runtime/state/pipeline_state.sqlite}
    secrets:
      object_storage_access_key: ${ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY:-disabled}
      object_storage_secret_key: ${ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY:-disabled}
    service_auth:
      header_name: X-API-Key
      reader_api_key: ${ETL_IDENTITY_SERVICE_READER_API_KEY:-}
      operator_api_key: ${ETL_IDENTITY_SERVICE_OPERATOR_API_KEY:-}
      reader_tenant_id: ${ETL_IDENTITY_SERVICE_READER_TENANT_ID:-default}
      operator_tenant_id: ${ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID:-default}
""",
    )
    monkeypatch.delenv("ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_SERVICE_READER_API_KEY", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_SERVICE_OPERATOR_API_KEY", raising=False)

    environment = load_runtime_environment("container", runtime_config)

    assert environment.name == "container"
    assert environment.state_db is not None
    assert environment.state_db.as_posix().endswith("/runtime/state/pipeline_state.sqlite")
    assert environment.secrets == {
        "object_storage_access_key": "disabled",
        "object_storage_secret_key": "disabled",
    }
    assert environment.service_auth is None


def test_load_runtime_environment_resolves_only_selected_environment_placeholders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: container
environments:
  container:
    config_dir: .
    state_db: ${ETL_IDENTITY_STATE_DB:-/runtime/state/pipeline_state.sqlite}
    secrets:
      object_storage_access_key: ${ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY:-disabled}
      object_storage_secret_key: ${ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY:-disabled}
    service_auth:
      header_name: X-API-Key
      reader_api_key: ${ETL_IDENTITY_SERVICE_READER_API_KEY:-}
      operator_api_key: ${ETL_IDENTITY_SERVICE_OPERATOR_API_KEY:-}
      reader_tenant_id: ${ETL_IDENTITY_SERVICE_READER_TENANT_ID:-default}
      operator_tenant_id: ${ETL_IDENTITY_SERVICE_OPERATOR_TENANT_ID:-default}
  prod:
    config_dir: .
    state_db: ${TEST_STATE_DB}
    service_auth:
      mode: jwt
      header_name: Authorization
      issuer: ${TEST_SERVICE_JWT_ISSUER}
      audience: ${TEST_SERVICE_JWT_AUDIENCE}
      algorithms:
        - HS256
      jwt_secret: ${TEST_SERVICE_JWT_SECRET}
      tenant_claim_path: tenant_id
      reader_roles:
        - etl-reader
      operator_roles:
        - etl-operator
""",
    )
    monkeypatch.delenv("ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_SERVICE_READER_API_KEY", raising=False)
    monkeypatch.delenv("ETL_IDENTITY_SERVICE_OPERATOR_API_KEY", raising=False)
    monkeypatch.delenv("TEST_STATE_DB", raising=False)
    monkeypatch.delenv("TEST_SERVICE_JWT_ISSUER", raising=False)
    monkeypatch.delenv("TEST_SERVICE_JWT_AUDIENCE", raising=False)
    monkeypatch.delenv("TEST_SERVICE_JWT_SECRET", raising=False)

    environment = load_runtime_environment("container", runtime_config)

    assert environment.name == "container"
    assert environment.state_db is not None
    assert environment.state_db.as_posix().endswith("/runtime/state/pipeline_state.sqlite")


def test_load_runtime_environment_rejects_partial_service_auth_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_environment_file(runtime_config)
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_OBJECT_STORAGE_ACCESS_KEY", "access-key")
    monkeypatch.setenv("TEST_SERVICE_READER_API_KEY", "reader-key")
    monkeypatch.delenv("TEST_SERVICE_OPERATOR_API_KEY", raising=False)

    with pytest.raises(
        ValueError,
        match=r"runtime_environments\.yml: environments\.prod\.service_auth must define both reader_api_key and operator_api_key",
    ):
        load_runtime_environment("prod", runtime_config)


def test_load_runtime_environment_rejects_api_key_service_auth_without_tenant_mapping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: prod
environments:
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    service_auth:
      mode: api_key
      header_name: X-API-Key
      reader_api_key: ${TEST_SERVICE_READER_API_KEY}
      operator_api_key: ${TEST_SERVICE_OPERATOR_API_KEY}
      operator_tenant_id: tenant-a
""",
    )
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_SERVICE_READER_API_KEY", "reader-key")
    monkeypatch.setenv("TEST_SERVICE_OPERATOR_API_KEY", "operator-key")

    with pytest.raises(
        ValueError,
        match=r"runtime_environments\.yml: environments\.prod\.service_auth\.reader_tenant_id must be a non-empty string",
    ):
        load_runtime_environment("prod", runtime_config)


def test_load_runtime_environment_supports_jwt_service_auth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: prod
environments:
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    service_auth:
      mode: jwt
      header_name: Authorization
      issuer: ${TEST_SERVICE_JWT_ISSUER}
      audience: ${TEST_SERVICE_JWT_AUDIENCE}
      algorithms:
        - HS256
      jwt_secret: ${TEST_SERVICE_JWT_SECRET}
      role_claim: realm_access.roles
      scope_claim: scope
      tenant_claim_path: tenant_id
      subject_claim: preferred_username
      reader_roles:
        - etl-reader
      operator_roles:
        - etl-operator
      reader_scopes:
        - service:health
        - runs:read
      operator_scopes:
        - service:health
        - runs:read
        - review_cases:write
        - runs:replay
""",
    )
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_SERVICE_JWT_ISSUER", "https://idp.example.test")
    monkeypatch.setenv("TEST_SERVICE_JWT_AUDIENCE", "etl-identity-api")
    monkeypatch.setenv("TEST_SERVICE_JWT_SECRET", "shared-signing-secret-material-32b")

    environment = load_runtime_environment("prod", runtime_config)

    assert environment.service_auth is not None
    assert environment.service_auth.mode == "jwt"
    assert environment.service_auth.header_name == "Authorization"
    assert environment.service_auth.issuer == "https://idp.example.test"
    assert environment.service_auth.audience == "etl-identity-api"
    assert environment.service_auth.algorithms == ("HS256",)
    assert environment.service_auth.jwt_secret == "shared-signing-secret-material-32b"
    assert environment.service_auth.jwt_public_key_pem is None
    assert environment.service_auth.role_claim == "realm_access.roles"
    assert environment.service_auth.scope_claim == "scope"
    assert environment.service_auth.tenant_claim_path == "tenant_id"
    assert environment.service_auth.subject_claim == "preferred_username"
    assert environment.service_auth.reader_roles == ("etl-reader",)
    assert environment.service_auth.operator_roles == ("etl-operator",)
    assert environment.service_auth.reader_scopes == ("service:health", "runs:read")
    assert environment.service_auth.operator_scopes == (
        "service:health",
        "runs:read",
        "review_cases:write",
        "runs:replay",
    )


def test_load_runtime_environment_supports_field_authorization_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: prod
environments:
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    field_authorization:
      service.golden_record:
        first_name: mask
        phone: deny
      delivery.golden_records:
        address: mask
""",
    )
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))

    environment = load_runtime_environment("prod", runtime_config)

    assert environment.field_authorization is not None
    assert environment.field_authorization.surface_rules[SERVICE_GOLDEN_RECORD_SURFACE] == {
        "first_name": "mask",
        "phone": "deny",
    }
    assert environment.field_authorization.surface_rules[DELIVERY_GOLDEN_RECORDS_SURFACE] == {
        "address": "mask",
    }


def test_load_runtime_environment_rejects_invalid_field_authorization_field(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: prod
environments:
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    field_authorization:
      service.golden_record:
        middle_name: mask
""",
    )
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))

    with pytest.raises(
        ValueError,
        match=r"runtime_environments\.yml: environments\.prod\.field_authorization\.service\.golden_record contains unsupported keys: middle_name",
    ):
        load_runtime_environment("prod", runtime_config)


def test_load_runtime_environment_rejects_invalid_field_authorization_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: prod
environments:
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    field_authorization:
      service.golden_record:
        first_name: redact
""",
    )
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))

    with pytest.raises(
        ValueError,
        match=r"runtime_environments\.yml: environments\.prod\.field_authorization\.service\.golden_record\.first_name must be one of: allow, deny, mask",
    ):
        load_runtime_environment("prod", runtime_config)


def test_load_runtime_environment_rejects_jwt_service_auth_without_tenant_claim_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: prod
environments:
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    service_auth:
      mode: jwt
      header_name: Authorization
      issuer: ${TEST_SERVICE_JWT_ISSUER}
      audience: ${TEST_SERVICE_JWT_AUDIENCE}
      algorithms:
        - HS256
      jwt_secret: ${TEST_SERVICE_JWT_SECRET}
      reader_roles:
        - etl-reader
      operator_roles:
        - etl-operator
""",
    )
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_SERVICE_JWT_ISSUER", "https://idp.example.test")
    monkeypatch.setenv("TEST_SERVICE_JWT_AUDIENCE", "etl-identity-api")
    monkeypatch.setenv("TEST_SERVICE_JWT_SECRET", "shared-signing-secret-material-32b")

    with pytest.raises(
        ValueError,
        match=r"runtime_environments\.yml: environments\.prod\.service_auth\.tenant_claim_path must be a non-empty string",
    ):
        load_runtime_environment("prod", runtime_config)


def test_load_runtime_environment_supports_cjis_runtime_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: cjis
environments:
  cjis:
    config_dir: ${ETL_IDENTITY_CONFIG_DIR:-.}
    state_db: ${ETL_IDENTITY_STATE_DB}
    secrets:
      object_storage_access_key: ${ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY}
      object_storage_secret_key: ${ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY}
    service_auth:
      mode: jwt
      header_name: Authorization
      issuer: ${ETL_IDENTITY_SERVICE_JWT_ISSUER}
      audience: ${ETL_IDENTITY_SERVICE_JWT_AUDIENCE}
      algorithms:
        - RS256
      jwt_public_key_pem: ${ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM}
      tenant_claim_path: tenant_id
      reader_roles:
        - etl-identity-reader
      operator_roles:
        - etl-identity-operator
""",
    )
    public_key_path = tmp_path / "jwt-public.pem"
    public_key_pem = _generate_rsa_public_key_pem()
    public_key_path.write_text(public_key_pem, encoding="utf-8")
    monkeypatch.setenv("ETL_IDENTITY_STATE_DB", "postgresql+psycopg://etl_identity:secret@db.internal:5432/identity_state")
    access_key_path = tmp_path / "object-storage-access-key.txt"
    access_key_path.write_text("access-key", encoding="utf-8")
    secret_key_path = tmp_path / "object-storage-secret-key.txt"
    secret_key_path.write_text("secret-key", encoding="utf-8")
    monkeypatch.setenv("ETL_IDENTITY_OBJECT_STORAGE_ACCESS_KEY_FILE", str(access_key_path))
    monkeypatch.setenv("ETL_IDENTITY_OBJECT_STORAGE_SECRET_KEY_FILE", str(secret_key_path))
    monkeypatch.setenv("ETL_IDENTITY_SERVICE_JWT_ISSUER", "https://issuer.example.gov")
    monkeypatch.setenv("ETL_IDENTITY_SERVICE_JWT_AUDIENCE", "etl-identity-api")
    monkeypatch.setenv("ETL_IDENTITY_SERVICE_JWT_PUBLIC_KEY_PEM_FILE", str(public_key_path))

    environment = load_runtime_environment("cjis", runtime_config)

    assert environment.name == "cjis"
    assert environment.state_db == "postgresql+psycopg://etl_identity:secret@db.internal:5432/identity_state"
    assert environment.secrets == {
        "object_storage_access_key": "access-key",
        "object_storage_secret_key": "secret-key",
    }
    assert environment.service_auth is not None
    assert environment.service_auth.mode == "jwt"
    assert environment.service_auth.algorithms == ("RS256",)
    assert environment.service_auth.jwt_public_key_pem == public_key_pem.strip()
    assert environment.service_auth.tenant_claim_path == "tenant_id"


def test_load_runtime_environment_resolves_service_auth_from_secret_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_environment_file(runtime_config)
    reader_key_path = tmp_path / "reader-key.txt"
    reader_key_path.write_text("reader-key-from-file\n", encoding="utf-8")
    operator_key_path = tmp_path / "operator-key.txt"
    operator_key_path.write_text("operator-key-from-file\n", encoding="utf-8")
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_OBJECT_STORAGE_ACCESS_KEY", "access-key")
    monkeypatch.setenv("TEST_SERVICE_READER_API_KEY_FILE", str(reader_key_path))
    monkeypatch.setenv("TEST_SERVICE_OPERATOR_API_KEY_FILE", str(operator_key_path))
    monkeypatch.delenv("TEST_SERVICE_READER_API_KEY", raising=False)
    monkeypatch.delenv("TEST_SERVICE_OPERATOR_API_KEY", raising=False)

    environment = load_runtime_environment("prod", runtime_config)

    assert environment.service_auth is not None
    assert environment.service_auth.reader_api_key == "reader-key-from-file"
    assert environment.service_auth.operator_api_key == "operator-key-from-file"
    assert environment.service_auth.reader_tenant_id == "default"
    assert environment.service_auth.operator_tenant_id == "default"


def test_load_runtime_environment_rejects_missing_secret_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_environment_file(runtime_config)
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_OBJECT_STORAGE_ACCESS_KEY", "access-key")
    monkeypatch.setenv("TEST_SERVICE_READER_API_KEY_FILE", str(tmp_path / "missing-reader-key.txt"))
    monkeypatch.setenv("TEST_SERVICE_OPERATOR_API_KEY", "operator-key")
    monkeypatch.delenv("TEST_SERVICE_READER_API_KEY", raising=False)

    with pytest.raises(
        ValueError,
        match=r"references secret-file environment variable TEST_SERVICE_READER_API_KEY_FILE",
    ):
        load_runtime_environment("prod", runtime_config)


def test_evaluate_runtime_auth_material_reports_file_backed_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_environment_file(runtime_config)
    reader_key_path = tmp_path / "reader-key.txt"
    reader_key_path.write_text("reader-key-from-file\n", encoding="utf-8")
    operator_key_path = tmp_path / "operator-key.txt"
    operator_key_path.write_text("operator-key-from-file\n", encoding="utf-8")
    access_key_path = tmp_path / "access-key.txt"
    access_key_path.write_text("access-key-from-file\n", encoding="utf-8")
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_OBJECT_STORAGE_ACCESS_KEY_FILE", str(access_key_path))
    monkeypatch.setenv("TEST_SERVICE_READER_API_KEY_FILE", str(reader_key_path))
    monkeypatch.setenv("TEST_SERVICE_OPERATOR_API_KEY_FILE", str(operator_key_path))
    monkeypatch.delenv("TEST_OBJECT_STORAGE_ACCESS_KEY", raising=False)
    monkeypatch.delenv("TEST_SERVICE_READER_API_KEY", raising=False)
    monkeypatch.delenv("TEST_SERVICE_OPERATOR_API_KEY", raising=False)

    summary = evaluate_runtime_auth_material(
        "prod",
        runtime_config,
        max_secret_file_age_hours=24.0,
    )

    assert summary["status"] == "ok"
    checks = {check["check"]: check for check in summary["checks"]}
    assert checks["service_auth.reader_api_key"]["source"] == "env_file"
    assert checks["service_auth.operator_api_key"]["source"] == "env_file"
    assert checks["secret:object_storage_access_key"]["source"] == "env_file"
    assert checks["service_auth.reader_api_key"]["file_age_hours"] >= 0.0


def test_check_runtime_auth_material_cli_reports_errors_for_stale_secret_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_runtime_environment_file(runtime_config)
    reader_key_path = tmp_path / "reader-key.txt"
    reader_key_path.write_text("reader-key-from-file\n", encoding="utf-8")
    operator_key_path = tmp_path / "operator-key.txt"
    operator_key_path.write_text("operator-key-from-file\n", encoding="utf-8")
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_OBJECT_STORAGE_ACCESS_KEY", "access-key")
    monkeypatch.setenv("TEST_SERVICE_READER_API_KEY_FILE", str(reader_key_path))
    monkeypatch.setenv("TEST_SERVICE_OPERATOR_API_KEY_FILE", str(operator_key_path))
    monkeypatch.delenv("TEST_SERVICE_READER_API_KEY", raising=False)
    monkeypatch.delenv("TEST_SERVICE_OPERATOR_API_KEY", raising=False)

    with pytest.raises(ValueError, match=r"Runtime auth material check failed"):
        main(
            [
                "check-runtime-auth-material",
                "--environment",
                "prod",
                "--runtime-config",
                str(runtime_config),
                "--max-secret-file-age-hours",
                "0",
            ]
        )


def test_load_runtime_environment_rejects_invalid_jwt_service_auth_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: prod
environments:
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    service_auth:
      mode: jwt
      issuer: https://idp.example.test
      audience: etl-identity-api
      algorithms:
        - HS256
      jwt_secret: ${TEST_SERVICE_JWT_SECRET}
      tenant_claim_path: tenant_id
      reader_roles:
        - shared-role
      operator_roles:
        - shared-role
""",
    )
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_SERVICE_JWT_SECRET", "shared-signing-secret-material-32b")

    with pytest.raises(
        ValueError,
        match=r"runtime_environments\.yml: environments\.prod\.service_auth must use distinct reader_roles and operator_roles",
    ):
        load_runtime_environment("prod", runtime_config)


def test_load_runtime_environment_rejects_invalid_service_scope_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: prod
environments:
  prod:
    config_dir: ./config
    state_db: ${TEST_STATE_DB}
    service_auth:
      mode: api_key
      header_name: X-API-Key
      reader_api_key: ${TEST_SERVICE_READER_API_KEY}
      operator_api_key: ${TEST_SERVICE_OPERATOR_API_KEY}
      reader_tenant_id: tenant-a
      operator_tenant_id: tenant-a
      reader_scopes:
        - service:health
      operator_scopes:
        - runs:replay
""",
    )
    monkeypatch.setenv("TEST_STATE_DB", str(tmp_path / "state" / "prod.sqlite"))
    monkeypatch.setenv("TEST_SERVICE_READER_API_KEY", "reader-key")
    monkeypatch.setenv("TEST_SERVICE_OPERATOR_API_KEY", "operator-key")

    with pytest.raises(
        ValueError,
        match=r"runtime_environments\.yml: environments\.prod\.service_auth\.operator_scopes must include all reader_scopes",
    ):
        load_runtime_environment("prod", runtime_config)


def test_load_benchmark_fixture_configs_reads_capacity_targets(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_text(
        config_dir / "benchmark_fixtures.yml",
        """
benchmark_fixtures:
  - name: tiny
    description: Tiny benchmark fixture for integration tests.
    profile: small
    person_count: 48
    duplicate_rate: 0.25
    seed: 123
    formats:
      - csv
    capacity_targets:
      single_host_container:
        max_total_duration_seconds: 30.0
        min_normalize_records_per_second: 0.0
        min_match_candidate_pairs_per_second: 0.0
""",
    )

    fixtures = load_benchmark_fixture_configs(config_dir)

    assert set(fixtures) == {"tiny"}
    assert fixtures["tiny"].person_count == 48
    assert fixtures["tiny"].formats == ("csv",)
    assert fixtures["tiny"].mode == "batch"
    assert fixtures["tiny"].capacity_targets["single_host_container"].max_total_duration_seconds == 30.0
    assert fixtures["tiny"].capacity_targets["single_host_container"].runtime_environment is None
    assert fixtures["tiny"].capacity_targets["single_host_container"].state_store_backend == "sqlite"


def test_load_benchmark_fixture_configs_reads_event_stream_fixture(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_text(
        config_dir / "benchmark_fixtures.yml",
        """
benchmark_fixtures:
  - name: stream_tiny
    description: Tiny event-stream benchmark fixture.
    mode: event_stream
    profile: small
    person_count: 48
    duplicate_rate: 0.25
    seed: 123
    formats:
      - csv
    stream_batch_count: 2
    stream_events_per_batch: 4
    capacity_targets:
      single_host_container:
        max_total_duration_seconds: 30.0
        min_normalize_records_per_second: 0.0
        min_match_candidate_pairs_per_second: 0.0
""",
    )

    fixtures = load_benchmark_fixture_configs(config_dir)

    assert fixtures["stream_tiny"].mode == "event_stream"
    assert fixtures["stream_tiny"].stream_batch_count == 2
    assert fixtures["stream_tiny"].stream_events_per_batch == 4


def test_load_benchmark_fixture_configs_reads_clustered_target_metadata(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_text(
        config_dir / "benchmark_fixtures.yml",
        """
benchmark_fixtures:
  - name: cluster_stream
    description: Clustered continuous-ingest benchmark fixture.
    mode: event_stream
    profile: small
    person_count: 48
    duplicate_rate: 0.25
    seed: 123
    formats:
      - csv
    stream_batch_count: 2
    stream_events_per_batch: 4
    capacity_targets:
      cluster_postgresql_baseline:
        runtime_environment: cluster
        state_store_backend: postgresql
        max_total_duration_seconds: 30.0
        min_normalize_records_per_second: 0.0
        min_match_candidate_pairs_per_second: 0.0
        max_stream_batch_duration_seconds: 5.0
        max_p95_stream_batch_duration_seconds: 5.0
        min_stream_events_per_second: 1.0
""",
    )

    fixtures = load_benchmark_fixture_configs(config_dir)

    target = fixtures["cluster_stream"].capacity_targets["cluster_postgresql_baseline"]
    assert target.runtime_environment == "cluster"
    assert target.state_store_backend == "postgresql"
    assert target.max_stream_batch_duration_seconds == 5.0
    assert target.max_p95_stream_batch_duration_seconds == 5.0
    assert target.min_stream_events_per_second == 1.0


def test_load_benchmark_fixture_configs_rejects_stream_thresholds_for_batch_fixture(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "config"
    _write_text(
        config_dir / "benchmark_fixtures.yml",
        """
benchmark_fixtures:
  - name: invalid_batch
    description: Invalid batch fixture.
    profile: small
    person_count: 48
    duplicate_rate: 0.25
    seed: 123
    formats:
      - csv
    capacity_targets:
      cluster_postgresql_baseline:
        max_total_duration_seconds: 30.0
        min_normalize_records_per_second: 0.0
        min_match_candidate_pairs_per_second: 0.0
        max_stream_batch_duration_seconds: 5.0
""",
    )

    with pytest.raises(
        ConfigValidationError,
        match=r"stream thresholds require mode=event_stream",
    ):
        load_benchmark_fixture_configs(config_dir)


def test_cli_match_uses_runtime_environment_overlay(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write_valid_config(config_dir)
    _write_text(
        config_dir / "environments" / "prod" / "matching_rules.yml",
        """
weights:
  canonical_name: 1.0
  canonical_dob: 0.0
  canonical_phone: 0.0
  canonical_address: 0.0
""",
    )
    runtime_config = tmp_path / "runtime_environments.yml"
    _write_text(
        runtime_config,
        """
default_environment: dev
environments:
  prod:
    config_dir: ./config
""",
    )

    normalized_input = tmp_path / "normalized.csv"
    _write_csv(
        normalized_input,
        [
            {
                "source_record_id": "A-1",
                "person_entity_id": "P-1",
                "source_system": "source_a",
                "first_name": "JOHN",
                "last_name": "SMITH",
                "dob": "1985-03-12",
                "address": "123 MAIN ST",
                "phone": "5551234567",
                "canonical_name": "JOHN SMITH",
                "canonical_dob": "1985-03-12",
                "canonical_address": "123 MAIN ST",
                "canonical_phone": "5551234567",
            },
            {
                "source_record_id": "B-1",
                "person_entity_id": "P-1",
                "source_system": "source_b",
                "first_name": "JONATHAN",
                "last_name": "SMITH",
                "dob": "1985-04-12",
                "address": "123 MAIN STREET",
                "phone": "5551230000",
                "canonical_name": "JOHN SMITH",
                "canonical_dob": "1985-04-12",
                "canonical_address": "123 MAIN STREET",
                "canonical_phone": "5551230000",
            },
        ],
    )
    match_output = tmp_path / "candidate_scores.csv"

    assert (
        main(
            [
                "match",
                "--input",
                str(normalized_input),
                "--output",
                str(match_output),
                "--environment",
                "prod",
                "--runtime-config",
                str(runtime_config),
            ]
        )
        == 0
    )

    match_rows = _read_csv(match_output)
    assert len(match_rows) == 1
    assert match_rows[0]["decision"] == "auto_merge"
    assert match_rows[0]["score"] == "1.0"
