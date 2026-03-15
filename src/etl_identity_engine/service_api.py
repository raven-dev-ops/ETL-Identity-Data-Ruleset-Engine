"""Operator-facing service API over persisted SQL pipeline state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import asdict
from pathlib import Path
import re
import time
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Path as ApiPath, Query, Request
import jwt
from jwt.exceptions import InvalidTokenError
from pydantic import BaseModel, ConfigDict, Field

from etl_identity_engine import __version__
from etl_identity_engine.observability import emit_structured_log, seconds_since, utc_now
from etl_identity_engine.operator_actions import (
    apply_review_decision_operation,
    export_job_run_operation,
    publish_run_operation,
    replay_run_operation,
)
from etl_identity_engine.output_contracts import DELIVERY_CONTRACT_VERSION
from etl_identity_engine.runtime_config import ExportJobConfig, ServiceAuthConfig, load_export_job_configs
from etl_identity_engine.storage.sqlite_store import (
    ExportJobRunRecord,
    PersistedReviewCase,
    PipelineStateStore,
    PipelineRunRecord,
    StoreOperationalMetrics,
)
from etl_identity_engine.storage.state_store_target import resolve_state_store_target


ReviewCaseStatus = Literal["pending", "approved", "rejected", "deferred"]
RunStatus = Literal["running", "completed", "failed"]
ServiceRole = Literal["reader", "operator"]
ServiceScope = Literal[
    "service:health",
    "service:metrics",
    "runs:read",
    "runs:replay",
    "runs:publish",
    "golden:read",
    "crosswalk:read",
    "public_safety:read",
    "review_cases:read",
    "review_cases:write",
    "exports:run",
]


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    role: ServiceRole
    auth_mode: Literal["api_key", "jwt"]
    subject: str | None = None
    scopes: tuple[str, ...] = ()


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"]
    state_db: str
    api_version: str
    service_started_at_utc: str


class ReadinessResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ready"]
    state_db: str
    api_version: str
    latest_completed_run_id: str | None
    latest_failed_run_id: str | None
    running_run_count: int
    audit_event_count: int


class RunStatusCountsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    running: int
    completed: int
    failed: int


class ReviewCaseStatusCountsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pending: int
    approved: int
    rejected: int
    deferred: int


class ServiceMetricsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_version: str
    state_db: str
    service_started_at_utc: str
    service_uptime_seconds: float
    runs: RunStatusCountsResponse
    exports: RunStatusCountsResponse
    review_cases: ReviewCaseStatusCountsResponse
    audit_event_count: int
    latest_completed_run_id: str | None
    latest_completed_run_finished_at_utc: str | None
    latest_failed_run_id: str | None
    latest_failed_run_finished_at_utc: str | None


class PageMetadataResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page_size: int
    total_count: int
    next_page_token: str | None
    sort: str


class RunStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    run_key: str
    attempt_number: int
    batch_id: str | None
    input_mode: str
    manifest_path: str | None
    base_dir: str
    config_dir: str | None
    profile: str | None
    seed: int | None
    formats: str | None
    status: RunStatus
    started_at_utc: str
    finished_at_utc: str
    total_records: int
    candidate_pair_count: int
    cluster_count: int
    golden_record_count: int
    review_queue_count: int
    failure_detail: str | None
    resumed_from_run_id: str | None
    summary: dict[str, Any] = Field(default_factory=dict)


class GoldenRecordResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    golden_id: str
    first_name: str
    first_name_source_record_id: str
    first_name_source_system: str
    first_name_rule_name: str
    last_name: str
    last_name_source_record_id: str
    last_name_source_system: str
    last_name_rule_name: str
    dob: str
    dob_source_record_id: str
    dob_source_system: str
    dob_rule_name: str
    address: str
    address_source_record_id: str
    address_source_system: str
    address_rule_name: str
    phone: str
    phone_source_record_id: str
    phone_source_system: str
    phone_rule_name: str
    person_entity_id: str
    cluster_id: str
    source_record_count: str


class CrosswalkLookupResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_record_id: str
    source_system: str
    person_entity_id: str
    cluster_id: str
    golden_id: str


class PublicSafetyGoldenActivityResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    golden_id: str
    cluster_id: str
    person_entity_id: str
    golden_first_name: str
    golden_last_name: str
    cad_incident_count: str
    rms_incident_count: str
    total_incident_count: str
    linked_source_record_count: str
    roles: str
    latest_incident_at: str


class PublicSafetyGoldenActivityListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[PublicSafetyGoldenActivityResponse]
    page: PageMetadataResponse


class PublicSafetyIncidentIdentityResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: str
    incident_source_system: str
    occurred_at: str
    incident_location: str
    incident_city: str
    incident_state: str
    incident_role: str
    person_entity_id: str
    source_record_id: str
    person_source_system: str
    golden_id: str
    cluster_id: str
    golden_first_name: str
    golden_last_name: str
    golden_dob: str
    golden_address: str
    golden_phone: str


class PublicSafetyIncidentIdentityListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[PublicSafetyIncidentIdentityResponse]
    page: PageMetadataResponse


class RunListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[RunStatusResponse]
    page: PageMetadataResponse


class GoldenRecordListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[GoldenRecordResponse]
    page: PageMetadataResponse


class ReviewCaseResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    review_id: str
    left_id: str
    right_id: str
    score: float
    reason_codes: str
    top_contributing_match_signals: str
    queue_status: ReviewCaseStatus
    assigned_to: str
    operator_notes: str
    created_at_utc: str
    updated_at_utc: str
    resolved_at_utc: str


class ReviewCaseListPageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ReviewCaseResponse]
    page: PageMetadataResponse


class ReviewDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: ReviewCaseStatus
    assigned_to: str | None = None
    notes: str | None = None


class ReviewDecisionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["noop", "updated"]
    case: ReviewCaseResponse


class ReplayRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_dir: str | None = None
    refresh_mode: Literal["full", "incremental"] | None = None


class ReplayRunResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["replayed", "reused_completed_run"]
    requested_run_id: str
    result_run_id: str
    state_db: str
    base_dir: str
    refresh_mode: Literal["full", "incremental"]
    replay_command: list[str]
    source_run: RunStatusResponse
    result_run: RunStatusResponse


class PublishRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_dir: str = Field(min_length=1)
    contract_version: str = Field(default=DELIVERY_CONTRACT_VERSION, min_length=1)


class PublishRunResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["published", "reused_snapshot"]
    run_id: str
    contract_version: str
    snapshot_dir: str
    current_pointer_path: str


class ExportJobResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    consumer: str
    description: str
    output_root: str
    contract_name: str
    contract_version: str
    format: str


class ExportJobRunResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    export_run_id: str
    export_key: str
    attempt_number: int
    job_name: str
    source_run_id: str
    contract_name: str
    contract_version: str
    output_root: str
    status: RunStatus
    started_at_utc: str
    finished_at_utc: str
    snapshot_dir: str
    current_pointer_path: str
    row_counts: dict[str, int] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    failure_detail: str | None = None


class ExportJobTriggerResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["exported", "reused_completed_export"]
    job: ExportJobResponse
    export_run: ExportJobRunResponse


def _serialize_run(record: PipelineRunRecord) -> RunStatusResponse:
    return RunStatusResponse.model_validate(asdict(record))


def _serialize_review_case(case: PersistedReviewCase) -> ReviewCaseResponse:
    return ReviewCaseResponse.model_validate(asdict(case))


def _serialize_export_job(job: ExportJobConfig) -> ExportJobResponse:
    return ExportJobResponse(
        name=job.name,
        consumer=job.consumer,
        description=job.description,
        output_root=str(job.output_root),
        contract_name=job.contract_name,
        contract_version=job.contract_version,
        format=job.export_format,
    )


def _serialize_export_run(record: ExportJobRunRecord) -> ExportJobRunResponse:
    return ExportJobRunResponse(
        export_run_id=record.export_run_id,
        export_key=record.export_key,
        attempt_number=record.attempt_number,
        job_name=record.job_name,
        source_run_id=record.source_run_id,
        contract_name=record.contract_name,
        contract_version=record.contract_version,
        output_root=record.output_root,
        status=record.status,
        started_at_utc=record.started_at_utc,
        finished_at_utc=record.finished_at_utc,
        snapshot_dir=record.snapshot_dir,
        current_pointer_path=record.current_pointer_path,
        row_counts=record.row_counts,
        metadata=record.metadata,
        failure_detail=record.failure_detail,
    )


def _serialize_page(
    *,
    page_size: int,
    total_count: int,
    next_page_token: str | None,
    sort: str,
) -> PageMetadataResponse:
    return PageMetadataResponse(
        page_size=page_size,
        total_count=total_count,
        next_page_token=next_page_token,
        sort=sort,
    )


def _resolve_not_found(error: FileNotFoundError) -> None:
    raise HTTPException(status_code=404, detail=str(error)) from error


def _resolve_operation_conflict(error: ValueError) -> None:
    raise HTTPException(status_code=409, detail=str(error)) from error


def _parse_page_token(page_token: str | None) -> int:
    if page_token is None:
        return 0
    return int(page_token)


def _serialize_metrics(
    metrics: StoreOperationalMetrics,
    *,
    state_db: str,
    service_started_at_utc: str,
    service_started_monotonic: float,
) -> ServiceMetricsResponse:
    return ServiceMetricsResponse(
        api_version=__version__,
        state_db=state_db,
        service_started_at_utc=service_started_at_utc,
        service_uptime_seconds=seconds_since(service_started_monotonic),
        runs=RunStatusCountsResponse(
            running=metrics.run_status_counts["running"],
            completed=metrics.run_status_counts["completed"],
            failed=metrics.run_status_counts["failed"],
        ),
        exports=RunStatusCountsResponse(
            running=metrics.export_status_counts["running"],
            completed=metrics.export_status_counts["completed"],
            failed=metrics.export_status_counts["failed"],
        ),
        review_cases=ReviewCaseStatusCountsResponse(
            pending=metrics.review_case_status_counts["pending"],
            approved=metrics.review_case_status_counts["approved"],
            rejected=metrics.review_case_status_counts["rejected"],
            deferred=metrics.review_case_status_counts["deferred"],
        ),
        audit_event_count=metrics.audit_event_count,
        latest_completed_run_id=metrics.latest_completed_run_id,
        latest_completed_run_finished_at_utc=metrics.latest_completed_run_finished_at_utc,
        latest_failed_run_id=metrics.latest_failed_run_id,
        latest_failed_run_finished_at_utc=metrics.latest_failed_run_finished_at_utc,
    )


def _extract_claim_value(claims: Mapping[str, Any], claim_path: str) -> Any:
    current: Any = claims
    for segment in claim_path.split("."):
        if not isinstance(current, Mapping) or segment not in current:
            return None
        current = current[segment]
    return current


def _normalize_claim_values(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {
            item
            for item in re.split(r"[\s,]+", value.strip())
            if item
        }
    if isinstance(value, (list, tuple, set)):
        return {
            item.strip()
            for item in value
            if isinstance(item, str) and item.strip()
        }
    return set()


def _authenticate_api_key(
    service_auth: ServiceAuthConfig,
    *,
    header_value: str | None,
) -> AuthenticatedPrincipal:
    if header_value is None or not header_value.strip():
        raise HTTPException(
            status_code=401,
            detail=f"Missing API key in header {service_auth.header_name}",
        )

    normalized_key = header_value.strip()
    if normalized_key == service_auth.reader_api_key:
        return AuthenticatedPrincipal(
            role="reader",
            auth_mode="api_key",
            scopes=service_auth.reader_scopes,
        )
    if normalized_key == service_auth.operator_api_key:
        return AuthenticatedPrincipal(
            role="operator",
            auth_mode="api_key",
            scopes=service_auth.operator_scopes,
        )
    raise HTTPException(status_code=401, detail="Invalid API key")


def _authenticate_jwt_bearer(
    service_auth: ServiceAuthConfig,
    *,
    header_value: str | None,
) -> AuthenticatedPrincipal:
    if header_value is None or not header_value.strip():
        raise HTTPException(
            status_code=401,
            detail=f"Missing bearer token in header {service_auth.header_name}",
        )

    scheme, _, token = header_value.strip().partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(
            status_code=401,
            detail=f"Expected Bearer token in header {service_auth.header_name}",
        )

    signing_key = service_auth.jwt_public_key_pem or service_auth.jwt_secret
    if signing_key is None:
        raise HTTPException(status_code=500, detail="JWT service authentication is misconfigured")

    try:
        claims = jwt.decode(
            token.strip(),
            signing_key,
            algorithms=list(service_auth.algorithms),
            audience=service_auth.audience,
            issuer=service_auth.issuer,
        )
    except InvalidTokenError as error:
        raise HTTPException(status_code=401, detail=f"Invalid bearer token: {error}") from error

    mapped_roles = _normalize_claim_values(_extract_claim_value(claims, service_auth.role_claim))
    claimed_scopes = _normalize_claim_values(_extract_claim_value(claims, service_auth.scope_claim))
    subject_value = _extract_claim_value(claims, service_auth.subject_claim)
    subject = subject_value.strip() if isinstance(subject_value, str) and subject_value.strip() else None
    if mapped_roles & set(service_auth.operator_roles):
        granted_scopes = tuple(sorted(claimed_scopes or set(service_auth.operator_scopes)))
        return AuthenticatedPrincipal(
            role="operator",
            auth_mode="jwt",
            subject=subject,
            scopes=granted_scopes,
        )
    if mapped_roles & set(service_auth.reader_roles):
        granted_scopes = tuple(sorted(claimed_scopes or set(service_auth.reader_scopes)))
        return AuthenticatedPrincipal(
            role="reader",
            auth_mode="jwt",
            subject=subject,
            scopes=granted_scopes,
        )
    raise HTTPException(
        status_code=403,
        detail=(
            "Bearer token claims do not map to a permitted service role via "
            f"{service_auth.role_claim}"
        ),
    )


def create_service_app(
    state_db_path: str | Path,
    *,
    service_auth: ServiceAuthConfig,
    config_dir: Path | None = None,
    environment: str | None = None,
) -> FastAPI:
    state_target = resolve_state_store_target(state_db_path)
    if state_target.file_path is not None and not state_target.file_path.exists():
        raise FileNotFoundError(f"Persisted state database not found: {state_target.display_name}")

    state_db_display = state_target.display_name
    store = PipelineStateStore(state_target.raw_value)
    export_jobs = load_export_job_configs(config_dir, environment=environment)
    app = FastAPI(
        title="ETL Identity Engine Operator API",
        version=__version__,
        summary="Authenticated operator and consumer service over persisted pipeline state.",
    )
    service_started_at_utc = utc_now()
    service_started_monotonic = time.perf_counter()

    @app.middleware("http")
    async def _structured_request_logging(request: Request, call_next):
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            emit_structured_log(
                "service_request_failed",
                component="service_api",
                method=request.method,
                path=request.url.path,
                duration_seconds=seconds_since(started),
                principal_role=getattr(request.state, "principal_role", "anonymous"),
                principal_subject=getattr(request.state, "principal_subject", ""),
                principal_scopes=getattr(request.state, "principal_scopes", []),
                auth_mode=getattr(request.state, "principal_auth_mode", "none"),
            )
            raise

        emit_structured_log(
            "service_request_completed",
            component="service_api",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_seconds=seconds_since(started),
            principal_role=getattr(request.state, "principal_role", "anonymous"),
            principal_subject=getattr(request.state, "principal_subject", ""),
            principal_scopes=getattr(request.state, "principal_scopes", []),
            auth_mode=getattr(request.state, "principal_auth_mode", "none"),
        )
        return response

    def require_access(
        *allowed_roles: ServiceRole,
        required_scopes: tuple[ServiceScope, ...],
    ):
        allowed = set(allowed_roles)
        required = set(required_scopes)

        def dependency(
            request: Request,
            auth_header: str | None = Header(default=None, alias=service_auth.header_name),
        ) -> AuthenticatedPrincipal:
            if service_auth.mode == "jwt":
                principal = _authenticate_jwt_bearer(service_auth, header_value=auth_header)
            else:
                principal = _authenticate_api_key(service_auth, header_value=auth_header)

            if principal.role not in allowed:
                raise HTTPException(
                    status_code=403,
                    detail=f"Role {principal.role!r} is not permitted for this operation",
                )
            granted_scopes = set(principal.scopes)
            missing_scopes = sorted(required - granted_scopes)
            if missing_scopes:
                raise HTTPException(
                    status_code=403,
                    detail=(
                        "Authenticated principal is missing required scopes: "
                        + ", ".join(missing_scopes)
                    ),
                )
            request.state.principal_role = principal.role
            request.state.principal_subject = principal.subject or ""
            request.state.principal_scopes = list(principal.scopes)
            request.state.principal_auth_mode = principal.auth_mode
            return principal

        return dependency

    health_access = require_access("reader", "operator", required_scopes=("service:health",))
    metrics_access = require_access("reader", "operator", required_scopes=("service:metrics",))
    run_read_access = require_access("reader", "operator", required_scopes=("runs:read",))
    golden_read_access = require_access("reader", "operator", required_scopes=("golden:read",))
    crosswalk_read_access = require_access("reader", "operator", required_scopes=("crosswalk:read",))
    public_safety_read_access = require_access(
        "reader",
        "operator",
        required_scopes=("public_safety:read",),
    )
    review_read_access = require_access("reader", "operator", required_scopes=("review_cases:read",))
    review_write_access = require_access("operator", required_scopes=("review_cases:write",))
    replay_access = require_access("operator", required_scopes=("runs:replay",))
    publish_access = require_access("operator", required_scopes=("runs:publish",))
    export_access = require_access("operator", required_scopes=("exports:run",))

    @app.get("/healthz", response_model=HealthResponse, tags=["health"])
    def healthz(
        _principal: AuthenticatedPrincipal = Depends(health_access),
    ) -> HealthResponse:
        return HealthResponse(
            status="ok",
            state_db=state_db_display,
            api_version=__version__,
            service_started_at_utc=service_started_at_utc,
        )

    @app.get("/readyz", response_model=ReadinessResponse, tags=["health"])
    def readyz(
        _principal: AuthenticatedPrincipal = Depends(health_access),
    ) -> ReadinessResponse:
        metrics = store.load_operational_metrics()
        return ReadinessResponse(
            status="ready",
            state_db=state_db_display,
            api_version=__version__,
            latest_completed_run_id=metrics.latest_completed_run_id,
            latest_failed_run_id=metrics.latest_failed_run_id,
            running_run_count=metrics.run_status_counts["running"],
            audit_event_count=metrics.audit_event_count,
        )

    @app.get("/api/v1/metrics", response_model=ServiceMetricsResponse, tags=["health"])
    def metrics(
        _principal: AuthenticatedPrincipal = Depends(metrics_access),
    ) -> ServiceMetricsResponse:
        return _serialize_metrics(
            store.load_operational_metrics(),
            state_db=state_db_display,
            service_started_at_utc=service_started_at_utc,
            service_started_monotonic=service_started_monotonic,
        )

    @app.get("/api/v1/runs", response_model=RunListResponse, tags=["runs"])
    def list_runs(
        status: Annotated[RunStatus | None, Query()] = None,
        input_mode: Annotated[str | None, Query(min_length=1)] = None,
        batch_id: Annotated[str | None, Query(min_length=1)] = None,
        query: Annotated[str | None, Query(min_length=1)] = None,
        sort: Annotated[
            Literal["finished_at_desc", "finished_at_asc", "started_at_desc", "started_at_asc"],
            Query(),
        ] = "finished_at_desc",
        page_size: Annotated[int, Query(ge=1, le=100)] = 50,
        page_token: Annotated[str | None, Query(pattern=r"^\d+$")] = None,
        _principal: AuthenticatedPrincipal = Depends(run_read_access),
    ) -> RunListResponse:
        offset = _parse_page_token(page_token)
        result = store.list_run_records(
            status=status,
            input_mode=input_mode,
            batch_id=batch_id,
            search_query=query,
            sort=sort,
            limit=page_size,
            offset=offset,
        )
        return RunListResponse(
            items=[_serialize_run(record) for record in result.items],
            page=_serialize_page(
                page_size=page_size,
                total_count=result.total_count,
                next_page_token=result.next_page_token,
                sort=sort,
            ),
        )

    @app.get("/api/v1/runs/latest", response_model=RunStatusResponse, tags=["runs"])
    def get_latest_completed_run(
        _principal: AuthenticatedPrincipal = Depends(run_read_access),
    ) -> RunStatusResponse:
        run_id = store.latest_completed_run_id()
        if run_id is None:
            raise HTTPException(status_code=404, detail=f"No completed persisted runs found in {state_db_display}")
        return _serialize_run(store.load_run_record(run_id))

    @app.get("/api/v1/runs/{run_id}", response_model=RunStatusResponse, tags=["runs"])
    def get_run(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        _principal: AuthenticatedPrincipal = Depends(run_read_access),
    ) -> RunStatusResponse:
        try:
            return _serialize_run(store.load_run_record(run_id))
        except FileNotFoundError as error:
            _resolve_not_found(error)

    @app.get(
        "/api/v1/runs/{run_id}/golden-records",
        response_model=GoldenRecordListResponse,
        tags=["golden"],
    )
    def list_golden_records(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        cluster_id: Annotated[str | None, Query(min_length=1)] = None,
        person_entity_id: Annotated[str | None, Query(min_length=1)] = None,
        query: Annotated[str | None, Query(min_length=1)] = None,
        sort: Annotated[
            Literal["golden_id_asc", "golden_id_desc", "last_name_asc", "last_name_desc"],
            Query(),
        ] = "golden_id_asc",
        page_size: Annotated[int, Query(ge=1, le=100)] = 50,
        page_token: Annotated[str | None, Query(pattern=r"^\d+$")] = None,
        _principal: AuthenticatedPrincipal = Depends(golden_read_access),
    ) -> GoldenRecordListResponse:
        try:
            store.load_run_record(run_id)
        except FileNotFoundError as error:
            _resolve_not_found(error)
        offset = _parse_page_token(page_token)
        result = store.list_golden_records(
            run_id=run_id,
            cluster_id=cluster_id,
            person_entity_id=person_entity_id,
            search_query=query,
            sort=sort,
            limit=page_size,
            offset=offset,
        )
        return GoldenRecordListResponse(
            items=[GoldenRecordResponse.model_validate(item) for item in result.items],
            page=_serialize_page(
                page_size=page_size,
                total_count=result.total_count,
                next_page_token=result.next_page_token,
                sort=sort,
            ),
        )

    @app.get(
        "/api/v1/runs/{run_id}/golden-records/{golden_id}",
        response_model=GoldenRecordResponse,
        tags=["golden"],
    )
    def get_golden_record(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        golden_id: Annotated[str, ApiPath(min_length=1, pattern=r"^G-.+")],
        _principal: AuthenticatedPrincipal = Depends(golden_read_access),
    ) -> GoldenRecordResponse:
        try:
            return GoldenRecordResponse.model_validate(
                store.load_golden_record(run_id=run_id, golden_id=golden_id)
            )
        except FileNotFoundError as error:
            _resolve_not_found(error)

    @app.get(
        "/api/v1/runs/{run_id}/crosswalk/source-records/{source_record_id}",
        response_model=CrosswalkLookupResponse,
        tags=["crosswalk"],
    )
    def get_crosswalk_record_for_source(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        source_record_id: Annotated[str, ApiPath(min_length=1)],
        _principal: AuthenticatedPrincipal = Depends(crosswalk_read_access),
    ) -> CrosswalkLookupResponse:
        try:
            return CrosswalkLookupResponse.model_validate(
                store.load_crosswalk_record_for_source(
                    run_id=run_id,
                    source_record_id=source_record_id,
                )
            )
        except FileNotFoundError as error:
            _resolve_not_found(error)

    @app.get(
        "/api/v1/runs/{run_id}/public-safety/golden-activity",
        response_model=PublicSafetyGoldenActivityListResponse,
        tags=["public-safety"],
    )
    def list_public_safety_golden_activity(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        person_entity_id: Annotated[str | None, Query(min_length=1)] = None,
        query: Annotated[str | None, Query(min_length=1)] = None,
        sort: Annotated[
            Literal[
                "total_incident_desc",
                "total_incident_asc",
                "latest_incident_desc",
                "latest_incident_asc",
                "golden_id_asc",
                "golden_id_desc",
            ],
            Query(),
        ] = "total_incident_desc",
        page_size: Annotated[int, Query(ge=1, le=100)] = 50,
        page_token: Annotated[str | None, Query(pattern=r"^\d+$")] = None,
        _principal: AuthenticatedPrincipal = Depends(public_safety_read_access),
    ) -> PublicSafetyGoldenActivityListResponse:
        try:
            store.load_run_record(run_id)
        except FileNotFoundError as error:
            _resolve_not_found(error)
        offset = _parse_page_token(page_token)
        result = store.list_public_safety_golden_activity(
            run_id=run_id,
            person_entity_id=person_entity_id,
            search_query=query,
            sort=sort,
            limit=page_size,
            offset=offset,
        )
        return PublicSafetyGoldenActivityListResponse(
            items=[
                PublicSafetyGoldenActivityResponse.model_validate(item)
                for item in result.items
            ],
            page=_serialize_page(
                page_size=page_size,
                total_count=result.total_count,
                next_page_token=result.next_page_token,
                sort=sort,
            ),
        )

    @app.get(
        "/api/v1/runs/{run_id}/public-safety/golden-activity/{golden_id}",
        response_model=PublicSafetyGoldenActivityResponse,
        tags=["public-safety"],
    )
    def get_public_safety_golden_activity(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        golden_id: Annotated[str, ApiPath(min_length=1, pattern=r"^G-.+")],
        _principal: AuthenticatedPrincipal = Depends(public_safety_read_access),
    ) -> PublicSafetyGoldenActivityResponse:
        try:
            return PublicSafetyGoldenActivityResponse.model_validate(
                store.load_public_safety_golden_activity(run_id=run_id, golden_id=golden_id)
            )
        except FileNotFoundError as error:
            _resolve_not_found(error)

    @app.get(
        "/api/v1/runs/{run_id}/public-safety/incidents",
        response_model=PublicSafetyIncidentIdentityListResponse,
        tags=["public-safety"],
    )
    def list_public_safety_incident_identity(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        golden_id: Annotated[str | None, Query(min_length=1)] = None,
        incident_id: Annotated[str | None, Query(min_length=1)] = None,
        incident_source_system: Annotated[str | None, Query(min_length=1)] = None,
        query: Annotated[str | None, Query(min_length=1)] = None,
        sort: Annotated[
            Literal[
                "occurred_at_desc",
                "occurred_at_asc",
                "incident_id_asc",
                "incident_id_desc",
            ],
            Query(),
        ] = "occurred_at_desc",
        page_size: Annotated[int, Query(ge=1, le=100)] = 50,
        page_token: Annotated[str | None, Query(pattern=r"^\d+$")] = None,
        _principal: AuthenticatedPrincipal = Depends(public_safety_read_access),
    ) -> PublicSafetyIncidentIdentityListResponse:
        try:
            store.load_run_record(run_id)
        except FileNotFoundError as error:
            _resolve_not_found(error)
        offset = _parse_page_token(page_token)
        result = store.list_public_safety_incident_identity(
            run_id=run_id,
            golden_id=golden_id,
            incident_id=incident_id,
            incident_source_system=incident_source_system,
            search_query=query,
            sort=sort,
            limit=page_size,
            offset=offset,
        )
        return PublicSafetyIncidentIdentityListResponse(
            items=[
                PublicSafetyIncidentIdentityResponse.model_validate(item)
                for item in result.items
            ],
            page=_serialize_page(
                page_size=page_size,
                total_count=result.total_count,
                next_page_token=result.next_page_token,
                sort=sort,
            ),
        )

    @app.get(
        "/api/v1/runs/{run_id}/review-cases/page",
        response_model=ReviewCaseListPageResponse,
        tags=["review-cases"],
    )
    def list_review_cases_page(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        status: Annotated[ReviewCaseStatus | None, Query()] = None,
        assigned_to: Annotated[str | None, Query(min_length=1)] = None,
        query: Annotated[str | None, Query(min_length=1)] = None,
        sort: Annotated[
            Literal[
                "queue_order_asc",
                "queue_order_desc",
                "score_desc",
                "score_asc",
                "updated_at_desc",
                "updated_at_asc",
            ],
            Query(),
        ] = "queue_order_asc",
        page_size: Annotated[int, Query(ge=1, le=100)] = 50,
        page_token: Annotated[str | None, Query(pattern=r"^\d+$")] = None,
        _principal: AuthenticatedPrincipal = Depends(review_read_access),
    ) -> ReviewCaseListPageResponse:
        try:
            store.load_run_record(run_id)
        except FileNotFoundError as error:
            _resolve_not_found(error)
        offset = _parse_page_token(page_token)
        result = store.list_review_cases(
            run_id=run_id,
            queue_status=status,
            assigned_to=assigned_to,
            search_query=query,
            sort=sort,
            limit=page_size,
            offset=offset,
        )
        return ReviewCaseListPageResponse(
            items=[_serialize_review_case(case) for case in result.items],
            page=_serialize_page(
                page_size=page_size,
                total_count=result.total_count,
                next_page_token=result.next_page_token,
                sort=sort,
            ),
        )

    @app.get(
        "/api/v1/runs/{run_id}/review-cases",
        response_model=list[ReviewCaseResponse],
        tags=["review-cases"],
    )
    def list_review_cases(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        status: Annotated[ReviewCaseStatus | None, Query()] = None,
        assigned_to: Annotated[str | None, Query(min_length=1)] = None,
        _principal: AuthenticatedPrincipal = Depends(review_read_access),
    ) -> list[ReviewCaseResponse]:
        return [
            _serialize_review_case(case)
            for case in store.list_review_cases(
                run_id=run_id,
                queue_status=status,
                assigned_to=assigned_to,
            )
        ]

    @app.get(
        "/api/v1/runs/{run_id}/review-cases/{review_id}",
        response_model=ReviewCaseResponse,
        tags=["review-cases"],
    )
    def get_review_case(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        review_id: Annotated[str, ApiPath(min_length=1, pattern=r"^REV-.+")],
        _principal: AuthenticatedPrincipal = Depends(review_read_access),
    ) -> ReviewCaseResponse:
        try:
            return _serialize_review_case(store.load_review_case(run_id=run_id, review_id=review_id))
        except FileNotFoundError as error:
            _resolve_not_found(error)

    @app.post(
        "/api/v1/runs/{run_id}/review-cases/{review_id}/decision",
        response_model=ReviewDecisionResponse,
        tags=["review-cases"],
    )
    def apply_review_decision(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        review_id: Annotated[str, ApiPath(min_length=1, pattern=r"^REV-.+")],
        request: ReviewDecisionRequest,
        principal: AuthenticatedPrincipal = Depends(review_write_access),
    ) -> ReviewDecisionResponse:
        try:
            result = apply_review_decision_operation(
                store=store,
                run_id=run_id,
                review_id=review_id,
                decision=request.decision,
                assigned_to=request.assigned_to,
                notes=request.notes,
            )
        except FileNotFoundError as error:
            store.record_audit_event(
                actor_type="service_api",
                actor_id=principal.subject or principal.role,
                action="apply_review_decision",
                resource_type="review_case",
                resource_id=review_id,
                run_id=run_id,
                status="failed",
                details={
                    "decision": request.decision,
                    "assigned_to": request.assigned_to or "",
                    "notes": request.notes or "",
                    "actor_role": principal.role,
                    "actor_subject": principal.subject or "",
                    "granted_scopes": list(principal.scopes),
                    "required_scopes": ["review_cases:write"],
                    "auth_mode": principal.auth_mode,
                    "error": str(error),
                },
            )
            _resolve_not_found(error)
        except ValueError as error:
            store.record_audit_event(
                actor_type="service_api",
                actor_id=principal.subject or principal.role,
                action="apply_review_decision",
                resource_type="review_case",
                resource_id=review_id,
                run_id=run_id,
                status="failed",
                details={
                    "decision": request.decision,
                    "assigned_to": request.assigned_to or "",
                    "notes": request.notes or "",
                    "actor_role": principal.role,
                    "actor_subject": principal.subject or "",
                    "granted_scopes": list(principal.scopes),
                    "required_scopes": ["review_cases:write"],
                    "auth_mode": principal.auth_mode,
                    "error": str(error),
                },
            )
            _resolve_operation_conflict(error)
        store.record_audit_event(
            actor_type="service_api",
            actor_id=principal.subject or principal.role,
            action="apply_review_decision",
            resource_type="review_case",
            resource_id=result.case.review_id,
            run_id=result.case.run_id,
            status="noop" if result.action == "noop" else "succeeded",
            details={
                "decision": result.case.queue_status,
                "assigned_to": result.case.assigned_to,
                "operator_notes": result.case.operator_notes,
                "action": result.action,
                "actor_role": principal.role,
                "actor_subject": principal.subject or "",
                "granted_scopes": list(principal.scopes),
                "required_scopes": ["review_cases:write"],
                "auth_mode": principal.auth_mode,
            },
        )
        emit_structured_log(
            "review_decision_applied",
            component="service_api",
            actor_role=principal.role,
            actor_subject=principal.subject or "",
            actor_scopes=list(principal.scopes),
            run_id=result.case.run_id,
            review_id=result.case.review_id,
            action=result.action,
            queue_status=result.case.queue_status,
        )
        return ReviewDecisionResponse(action=result.action, case=_serialize_review_case(result.case))

    @app.post(
        "/api/v1/runs/{run_id}/replay",
        response_model=ReplayRunResponse,
        tags=["runs"],
    )
    def replay_run(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        request: ReplayRunRequest,
        principal: AuthenticatedPrincipal = Depends(replay_access),
    ) -> ReplayRunResponse:
        try:
            from etl_identity_engine.cli import main as cli_main

            result = replay_run_operation(
                store=store,
                state_db=state_target.raw_value,
                source_run_id=run_id,
                base_dir=Path(request.base_dir) if request.base_dir else None,
                refresh_mode=request.refresh_mode,
                runner=cli_main,
            )
        except FileNotFoundError as error:
            store.record_audit_event(
                actor_type="service_api",
                actor_id=principal.subject or principal.role,
                action="replay_run",
                resource_type="pipeline_run",
                resource_id=run_id,
                run_id=run_id,
                status="failed",
                details={
                    "base_dir": request.base_dir or "",
                    "refresh_mode": request.refresh_mode or "",
                    "actor_role": principal.role,
                    "actor_subject": principal.subject or "",
                    "granted_scopes": list(principal.scopes),
                    "required_scopes": ["runs:replay"],
                    "auth_mode": principal.auth_mode,
                    "error": str(error),
                },
            )
            _resolve_not_found(error)
        except ValueError as error:
            store.record_audit_event(
                actor_type="service_api",
                actor_id=principal.subject or principal.role,
                action="replay_run",
                resource_type="pipeline_run",
                resource_id=run_id,
                run_id=run_id,
                status="failed",
                details={
                    "base_dir": request.base_dir or "",
                    "refresh_mode": request.refresh_mode or "",
                    "actor_role": principal.role,
                    "actor_subject": principal.subject or "",
                    "granted_scopes": list(principal.scopes),
                    "required_scopes": ["runs:replay"],
                    "auth_mode": principal.auth_mode,
                    "error": str(error),
                },
            )
            _resolve_operation_conflict(error)
        store.record_audit_event(
            actor_type="service_api",
            actor_id=principal.subject or principal.role,
            action="replay_run",
            resource_type="pipeline_run",
            resource_id=result.result_run.run_id,
            run_id=result.result_run.run_id,
            status="reused" if result.action == "reused_completed_run" else "succeeded",
            details={
                "requested_run_id": result.requested_run.run_id,
                "result_run_id": result.result_run.run_id,
                "refresh_mode": result.refresh_mode,
                "base_dir": str(result.base_dir),
                "replay_command": list(result.replay_command),
                "action": result.action,
                "actor_role": principal.role,
                "actor_subject": principal.subject or "",
                "granted_scopes": list(principal.scopes),
                "required_scopes": ["runs:replay"],
                "auth_mode": principal.auth_mode,
            },
        )
        emit_structured_log(
            "pipeline_run_replayed",
            component="service_api",
            actor_role=principal.role,
            actor_subject=principal.subject or "",
            actor_scopes=list(principal.scopes),
            requested_run_id=result.requested_run.run_id,
            result_run_id=result.result_run.run_id,
            refresh_mode=result.refresh_mode,
            action=result.action,
        )
        return ReplayRunResponse(
            action=result.action,
            requested_run_id=result.requested_run.run_id,
            result_run_id=result.result_run.run_id,
            state_db=str(result.state_db),
            base_dir=str(result.base_dir),
            refresh_mode=result.refresh_mode,
            replay_command=list(result.replay_command),
            source_run=_serialize_run(result.requested_run),
            result_run=_serialize_run(result.result_run),
        )

    @app.post(
        "/api/v1/runs/{run_id}/publish",
        response_model=PublishRunResponse,
        tags=["runs"],
    )
    def publish_run(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        request: PublishRunRequest,
        principal: AuthenticatedPrincipal = Depends(publish_access),
    ) -> PublishRunResponse:
        try:
            result = publish_run_operation(
                store=store,
                state_db=state_target.raw_value,
                run_id=run_id,
                output_dir=Path(request.output_dir),
                contract_version=request.contract_version,
            )
        except FileNotFoundError as error:
            store.record_audit_event(
                actor_type="service_api",
                actor_id=principal.subject or principal.role,
                action="publish_run",
                resource_type="pipeline_run",
                resource_id=run_id,
                run_id=run_id,
                status="failed",
                details={
                    "output_dir": request.output_dir,
                    "contract_version": request.contract_version,
                    "actor_role": principal.role,
                    "actor_subject": principal.subject or "",
                    "granted_scopes": list(principal.scopes),
                    "required_scopes": ["runs:publish"],
                    "auth_mode": principal.auth_mode,
                    "error": str(error),
                },
            )
            _resolve_not_found(error)
        except ValueError as error:
            store.record_audit_event(
                actor_type="service_api",
                actor_id=principal.subject or principal.role,
                action="publish_run",
                resource_type="pipeline_run",
                resource_id=run_id,
                run_id=run_id,
                status="failed",
                details={
                    "output_dir": request.output_dir,
                    "contract_version": request.contract_version,
                    "actor_role": principal.role,
                    "actor_subject": principal.subject or "",
                    "granted_scopes": list(principal.scopes),
                    "required_scopes": ["runs:publish"],
                    "auth_mode": principal.auth_mode,
                    "error": str(error),
                },
            )
            _resolve_operation_conflict(error)
        store.record_audit_event(
            actor_type="service_api",
            actor_id=principal.subject or principal.role,
            action="publish_run",
            resource_type="pipeline_run",
            resource_id=result.run.run_id,
            run_id=result.run.run_id,
            status="reused" if result.action == "reused_snapshot" else "succeeded",
            details={
                "output_dir": request.output_dir,
                "contract_version": result.contract_version,
                "snapshot_dir": str(result.snapshot_dir),
                "current_pointer_path": str(result.current_pointer_path),
                "action": result.action,
                "actor_role": principal.role,
                "actor_subject": principal.subject or "",
                "granted_scopes": list(principal.scopes),
                "required_scopes": ["runs:publish"],
                "auth_mode": principal.auth_mode,
            },
        )
        emit_structured_log(
            "delivery_snapshot_published",
            component="service_api",
            actor_role=principal.role,
            actor_subject=principal.subject or "",
            actor_scopes=list(principal.scopes),
            run_id=result.run.run_id,
            action=result.action,
            contract_version=result.contract_version,
            output_dir=request.output_dir,
            snapshot_dir=str(result.snapshot_dir),
        )
        return PublishRunResponse(
            action=result.action,
            run_id=result.run.run_id,
            contract_version=result.contract_version,
            snapshot_dir=str(result.snapshot_dir),
            current_pointer_path=str(result.current_pointer_path),
        )

    @app.post(
        "/api/v1/runs/{run_id}/exports/{job_name}",
        response_model=ExportJobTriggerResponse,
        tags=["exports"],
    )
    def trigger_export_job(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        job_name: Annotated[str, ApiPath(min_length=1)],
        principal: AuthenticatedPrincipal = Depends(export_access),
    ) -> ExportJobTriggerResponse:
        job = export_jobs.get(job_name)
        if job is None:
            error = FileNotFoundError(
                f"Configured export job not found: {job_name}. Available jobs: {sorted(export_jobs)}"
            )
            store.record_audit_event(
                actor_type="service_api",
                actor_id=principal.subject or principal.role,
                action="export_job_run",
                resource_type="export_job",
                resource_id=job_name,
                run_id=run_id,
                status="failed",
                details={
                    "job_name": job_name,
                    "source_run_id": run_id,
                    "actor_role": principal.role,
                    "actor_subject": principal.subject or "",
                    "granted_scopes": list(principal.scopes),
                    "required_scopes": ["exports:run"],
                    "auth_mode": principal.auth_mode,
                    "error": str(error),
                },
            )
            _resolve_not_found(error)
        try:
            result = export_job_run_operation(
                store=store,
                state_db=state_target.raw_value,
                source_run_id=run_id,
                job=job,
            )
        except FileNotFoundError as error:
            store.record_audit_event(
                actor_type="service_api",
                actor_id=principal.subject or principal.role,
                action="export_job_run",
                resource_type="export_job",
                resource_id=job_name,
                run_id=run_id,
                status="failed",
                details={
                    "job_name": job_name,
                    "source_run_id": run_id,
                    "actor_role": principal.role,
                    "actor_subject": principal.subject or "",
                    "granted_scopes": list(principal.scopes),
                    "required_scopes": ["exports:run"],
                    "auth_mode": principal.auth_mode,
                    "error": str(error),
                },
            )
            _resolve_not_found(error)
        except ValueError as error:
            store.record_audit_event(
                actor_type="service_api",
                actor_id=principal.subject or principal.role,
                action="export_job_run",
                resource_type="export_job",
                resource_id=job_name,
                run_id=run_id,
                status="failed",
                details={
                    "job_name": job_name,
                    "source_run_id": run_id,
                    "actor_role": principal.role,
                    "actor_subject": principal.subject or "",
                    "granted_scopes": list(principal.scopes),
                    "required_scopes": ["exports:run"],
                    "auth_mode": principal.auth_mode,
                    "error": str(error),
                },
            )
            _resolve_operation_conflict(error)
        store.record_audit_event(
            actor_type="service_api",
            actor_id=principal.subject or principal.role,
            action="export_job_run",
            resource_type="export_job",
            resource_id=result.job.name,
            run_id=result.source_run.run_id,
            status="reused" if result.action == "reused_completed_export" else "succeeded",
            details={
                "job_name": result.job.name,
                "source_run_id": result.source_run.run_id,
                "export_run_id": result.export_run.export_run_id,
                "snapshot_dir": result.export_run.snapshot_dir,
                "current_pointer_path": result.export_run.current_pointer_path,
                "action": result.action,
                "actor_role": principal.role,
                "actor_subject": principal.subject or "",
                "granted_scopes": list(principal.scopes),
                "required_scopes": ["exports:run"],
                "auth_mode": principal.auth_mode,
            },
        )
        emit_structured_log(
            "export_job_completed",
            component="service_api",
            actor_role=principal.role,
            actor_subject=principal.subject or "",
            actor_scopes=list(principal.scopes),
            job_name=result.job.name,
            source_run_id=result.source_run.run_id,
            export_run_id=result.export_run.export_run_id,
            action=result.action,
        )
        return ExportJobTriggerResponse(
            action=result.action,
            job=_serialize_export_job(result.job),
            export_run=_serialize_export_run(result.export_run),
        )

    return app
