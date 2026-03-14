"""Operator-facing service API over persisted SQLite pipeline state."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import FastAPI, HTTPException, Path as ApiPath, Query
from pydantic import BaseModel, ConfigDict, Field

from etl_identity_engine import __version__
from etl_identity_engine.storage.sqlite_store import (
    PersistedReviewCase,
    PipelineRunRecord,
    SQLitePipelineStore,
)


ReviewCaseStatus = Literal["pending", "approved", "rejected", "deferred"]
RunStatus = Literal["running", "completed", "failed"]


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"]
    state_db: str
    api_version: str


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


def _serialize_run(record: PipelineRunRecord) -> RunStatusResponse:
    return RunStatusResponse.model_validate(asdict(record))


def _serialize_review_case(case: PersistedReviewCase) -> ReviewCaseResponse:
    return ReviewCaseResponse.model_validate(asdict(case))


def _resolve_not_found(error: FileNotFoundError) -> None:
    raise HTTPException(status_code=404, detail=str(error)) from error


def create_service_app(state_db_path: Path | str) -> FastAPI:
    state_db = Path(state_db_path)
    if not state_db.exists():
        raise FileNotFoundError(f"Persisted state database not found: {state_db}")

    store = SQLitePipelineStore(state_db)
    app = FastAPI(
        title="ETL Identity Engine Operator API",
        version=__version__,
        summary="Read-only operator and consumer service over persisted pipeline state.",
    )

    @app.get("/healthz", response_model=HealthResponse, tags=["health"])
    def healthz() -> HealthResponse:
        return HealthResponse(status="ok", state_db=str(state_db.resolve()), api_version=__version__)

    @app.get("/api/v1/runs/latest", response_model=RunStatusResponse, tags=["runs"])
    def get_latest_completed_run() -> RunStatusResponse:
        run_id = store.latest_completed_run_id()
        if run_id is None:
            raise HTTPException(status_code=404, detail=f"No completed persisted runs found in {state_db}")
        return _serialize_run(store.load_run_record(run_id))

    @app.get("/api/v1/runs/{run_id}", response_model=RunStatusResponse, tags=["runs"])
    def get_run(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
    ) -> RunStatusResponse:
        try:
            return _serialize_run(store.load_run_record(run_id))
        except FileNotFoundError as error:
            _resolve_not_found(error)

    @app.get(
        "/api/v1/runs/{run_id}/golden-records/{golden_id}",
        response_model=GoldenRecordResponse,
        tags=["golden"],
    )
    def get_golden_record(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        golden_id: Annotated[str, ApiPath(min_length=1, pattern=r"^G-.+")],
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
        "/api/v1/runs/{run_id}/review-cases",
        response_model=list[ReviewCaseResponse],
        tags=["review-cases"],
    )
    def list_review_cases(
        run_id: Annotated[str, ApiPath(min_length=1, pattern=r"^RUN-.+")],
        status: Annotated[ReviewCaseStatus | None, Query()] = None,
        assigned_to: Annotated[str | None, Query(min_length=1)] = None,
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
    ) -> ReviewCaseResponse:
        try:
            return _serialize_review_case(store.load_review_case(run_id=run_id, review_id=review_id))
        except FileNotFoundError as error:
            _resolve_not_found(error)

    return app

