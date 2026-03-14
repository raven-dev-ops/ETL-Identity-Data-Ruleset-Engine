import csv
from pathlib import Path

import pytest

from etl_identity_engine.cli import main
from etl_identity_engine.runtime_config import (
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
""",
    )


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
        match=r"runtime_environments\.yml: configuration references required environment variable TEST_STATE_DB",
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
      subject_claim: preferred_username
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
    assert environment.service_auth.subject_claim == "preferred_username"
    assert environment.service_auth.reader_roles == ("etl-reader",)
    assert environment.service_auth.operator_roles == ("etl-operator",)


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
    assert fixtures["tiny"].capacity_targets["single_host_container"].max_total_duration_seconds == 30.0


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
