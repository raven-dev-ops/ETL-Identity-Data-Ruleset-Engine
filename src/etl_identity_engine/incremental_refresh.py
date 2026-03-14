"""Incremental refresh helpers for persisted manifest-driven runs."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import re

from etl_identity_engine.matching.blocking import BlockingPassMetric, blocking_key, generate_candidates_with_metrics
from etl_identity_engine.matching.clustering import cluster_links
from etl_identity_engine.review_cases import (
    apply_review_decisions,
    build_review_case_rows,
    build_review_override_map,
    review_pair_key,
)
from etl_identity_engine.matching.scoring import classify_score, explain_pair_score
from etl_identity_engine.runtime_config import PipelineConfig
from etl_identity_engine.storage.sqlite_store import PersistedRunBundle
from etl_identity_engine.survivorship.rules_engine import merge_records


@dataclass(frozen=True)
class IncrementalRefreshResult:
    match_rows: list[dict[str, str | float]]
    blocking_metrics_rows: list[dict[str, str | int]]
    clustered_rows: list[dict[str, str]]
    cluster_output_rows: list[dict[str, str]]
    golden_rows: list[dict[str, str]]
    crosswalk_rows: list[dict[str, str]]
    active_review_rows: list[dict[str, str | float]]
    review_rows: list[dict[str, str | float]]
    metadata: dict[str, object]


def _source_record_id(row: dict[str, object]) -> str:
    return str(row.get("source_record_id", "")).strip()


def _pair_key(left_id: str, right_id: str) -> tuple[str, str]:
    return tuple(sorted((left_id, right_id)))


def _override_decision(status: str) -> str:
    return "auto_merge" if status == "approved" else "no_match"


def _blocking_metrics_rows(
    blocking_metrics: list[BlockingPassMetric],
    *,
    overall_candidate_pair_count: int,
) -> list[dict[str, str | int]]:
    return [
        {
            "pass_name": metric.pass_name,
            "fields": ";".join(metric.fields),
            "raw_candidate_pair_count": metric.raw_candidate_pair_count,
            "new_candidate_pair_count": metric.new_candidate_pair_count,
            "cumulative_candidate_pair_count": metric.cumulative_candidate_pair_count,
            "overall_deduplicated_candidate_pair_count": overall_candidate_pair_count,
        }
        for metric in blocking_metrics
    ]


def _build_match_rows(
    rows: list[dict[str, str]],
    config: PipelineConfig,
    *,
    forced_pairs: set[tuple[str, str]] | None = None,
    review_overrides: dict[tuple[str, str], str] | None = None,
) -> tuple[list[dict[str, str | float]], list[BlockingPassMetric]]:
    blocking_passes = [blocking_pass.fields for blocking_pass in config.matching.blocking_passes]
    blocking_pass_names = [blocking_pass.name for blocking_pass in config.matching.blocking_passes]
    pairs, blocking_metrics = generate_candidates_with_metrics(
        rows,
        blocking_passes=blocking_passes,
        pass_names=blocking_pass_names,
    )
    rows_by_id = {_source_record_id(row): row for row in rows if _source_record_id(row)}
    seen_pairs = {
        review_pair_key(str(left.get("source_record_id", "")), str(right.get("source_record_id", "")))
        for left, right in pairs
    }
    for forced_pair in sorted(forced_pairs or set()):
        if forced_pair in seen_pairs:
            continue
        left_row = rows_by_id.get(forced_pair[0])
        right_row = rows_by_id.get(forced_pair[1])
        if left_row is None or right_row is None:
            continue
        pairs.append((left_row, right_row))
        seen_pairs.add(forced_pair)
    scored_rows: list[dict[str, str | float]] = []
    for left, right in pairs:
        detail = explain_pair_score(left, right, weights=config.matching.weights)
        scored_rows.append(
            {
                "left_id": left.get("source_record_id", ""),
                "right_id": right.get("source_record_id", ""),
                "score": detail.score,
                "decision": classify_score(
                    detail.score,
                    auto_merge=config.matching.thresholds.auto_merge,
                    manual_review_min=config.matching.thresholds.manual_review_min,
                    no_match_max=config.matching.thresholds.no_match_max,
                ),
                "matched_fields": ";".join(detail.matched_fields),
                "reason_trace": ";".join(detail.reason_trace),
            }
        )
    if review_overrides:
        scored_rows = apply_review_decisions(scored_rows, review_overrides)
    scored_rows.sort(key=lambda row: (str(row.get("left_id", "")), str(row.get("right_id", ""))))
    return scored_rows, blocking_metrics


def _build_crosswalk_rows(
    rows: list[dict[str, str]],
    golden_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    cluster_to_golden = {
        row.get("cluster_id", ""): row.get("golden_id", "")
        for row in golden_rows
        if row.get("cluster_id")
    }
    return [
        {
            "source_record_id": row.get("source_record_id", ""),
            "source_system": row.get("source_system", ""),
            "person_entity_id": row.get("person_entity_id", ""),
            "cluster_id": row.get("cluster_id", ""),
            "golden_id": cluster_to_golden.get(row.get("cluster_id", ""), ""),
        }
        for row in sorted(rows, key=lambda item: item.get("source_record_id", ""))
    ]


def _max_identifier_number(ids: set[str], prefix: str) -> int:
    pattern = re.compile(rf"^{re.escape(prefix)}-(\d+)$")
    max_number = 0
    for identifier in ids:
        match = pattern.match(identifier.strip())
        if match is None:
            continue
        max_number = max(max_number, int(match.group(1)))
    return max_number


def _build_previous_cluster_indexes(
    bundle: PersistedRunBundle,
) -> tuple[dict[str, str], dict[str, set[str]], dict[str, dict[str, str]]]:
    cluster_by_record: dict[str, str] = {}
    members_by_cluster: dict[str, set[str]] = defaultdict(set)
    row_by_record: dict[str, dict[str, str]] = {}
    for row in bundle.cluster_rows:
        source_record_id = str(row.get("source_record_id", "")).strip()
        cluster_id = str(row.get("cluster_id", "")).strip()
        if not source_record_id or not cluster_id:
            continue
        cluster_by_record[source_record_id] = cluster_id
        members_by_cluster[cluster_id].add(source_record_id)
        row_by_record[source_record_id] = {
            "source_record_id": source_record_id,
            "source_system": str(row.get("source_system", "")),
            "person_entity_id": str(row.get("person_entity_id", "")),
            "cluster_id": cluster_id,
        }
    return cluster_by_record, members_by_cluster, row_by_record


def _blocking_buckets(
    rows_by_id: dict[str, dict[str, str]],
    config: PipelineConfig,
) -> list[dict[tuple[str, ...], set[str]]]:
    buckets_per_pass: list[dict[tuple[str, ...], set[str]]] = []
    for blocking_pass in config.matching.blocking_passes:
        buckets: dict[tuple[str, ...], set[str]] = defaultdict(set)
        for record_id, row in rows_by_id.items():
            key = blocking_key(row, fields=blocking_pass.fields)
            if all(key):
                buckets[key].add(record_id)
        buckets_per_pass.append(buckets)
    return buckets_per_pass


def _collect_impacted_record_ids(
    *,
    current_rows_by_id: dict[str, dict[str, str]],
    previous_rows_by_id: dict[str, dict[str, str]],
    previous_cluster_by_record: dict[str, str],
    previous_cluster_members: dict[str, set[str]],
    review_override_pairs: set[tuple[str, str]],
    config: PipelineConfig,
) -> tuple[set[str], dict[str, int]]:
    current_ids = set(current_rows_by_id)
    previous_ids = set(previous_rows_by_id)

    inserted_ids = current_ids - previous_ids
    removed_ids = previous_ids - current_ids
    changed_ids = {
        record_id
        for record_id in current_ids & previous_ids
        if current_rows_by_id[record_id] != previous_rows_by_id[record_id]
    }

    impacted_ids: set[str] = set()
    review_neighbors: dict[str, set[str]] = defaultdict(set)
    for left_id, right_id in review_override_pairs:
        review_neighbors[left_id].add(right_id)
        review_neighbors[right_id].add(left_id)
    pending_record_ids: deque[str] = deque(
        sorted(
            inserted_ids
            | changed_ids
            | {
                record_id
                for pair in review_override_pairs
                for record_id in pair
                if record_id in current_rows_by_id
            }
        )
    )
    pending_cluster_ids: deque[str] = deque(
        sorted(
            {
                previous_cluster_by_record[record_id]
                for record_id in removed_ids | changed_ids
                if record_id in previous_cluster_by_record
            }
        )
    )
    indexed_buckets = _blocking_buckets(current_rows_by_id, config)
    processed_cluster_ids: set[str] = set()

    while pending_record_ids or pending_cluster_ids:
        while pending_cluster_ids:
            cluster_id = pending_cluster_ids.popleft()
            if cluster_id in processed_cluster_ids:
                continue
            processed_cluster_ids.add(cluster_id)
            for member_id in sorted(previous_cluster_members.get(cluster_id, ())):
                if member_id in current_rows_by_id and member_id not in impacted_ids:
                    pending_record_ids.append(member_id)

        if not pending_record_ids:
            continue

        record_id = pending_record_ids.popleft()
        if record_id in impacted_ids:
            continue

        impacted_ids.add(record_id)
        previous_cluster_id = previous_cluster_by_record.get(record_id)
        if previous_cluster_id and previous_cluster_id not in processed_cluster_ids:
            pending_cluster_ids.append(previous_cluster_id)

        row = current_rows_by_id[record_id]
        for neighbor_id in sorted(review_neighbors.get(record_id, ())):
            if neighbor_id in current_rows_by_id and neighbor_id not in impacted_ids:
                pending_record_ids.append(neighbor_id)
        for buckets, blocking_pass in zip(indexed_buckets, config.matching.blocking_passes, strict=True):
            key = blocking_key(row, fields=blocking_pass.fields)
            if not all(key):
                continue
            for neighbor_id in sorted(buckets.get(key, ())):
                if neighbor_id not in impacted_ids:
                    pending_record_ids.append(neighbor_id)

    return impacted_ids, {
        "inserted_record_count": len(inserted_ids),
        "changed_record_count": len(changed_ids),
        "removed_record_count": len(removed_ids),
    }


def _component_record_ids(
    record_ids: set[str],
    match_rows: list[dict[str, str | float]],
) -> list[list[str]]:
    accepted_links = [
        (str(row.get("left_id", "")), str(row.get("right_id", "")))
        for row in match_rows
        if row.get("decision") == "auto_merge"
    ]
    linked_components = cluster_links(accepted_links)
    assigned = {record_id for component in linked_components for record_id in component}
    all_components = linked_components + [[record_id] for record_id in sorted(record_ids) if record_id not in assigned]
    return sorted((sorted(component) for component in all_components), key=lambda component: component[0])


def _allocate_cluster_ids(
    components: list[list[str]],
    *,
    previous_cluster_by_record: dict[str, str],
    reserved_cluster_ids: set[str],
    previous_cluster_ids: set[str],
) -> dict[str, str]:
    assigned_cluster_ids: dict[str, str] = {}
    used_cluster_ids = set(reserved_cluster_ids)
    next_cluster_number = _max_identifier_number(previous_cluster_ids | reserved_cluster_ids, "C") + 1

    for component in components:
        candidate_cluster_ids = sorted(
            {
                previous_cluster_by_record[record_id]
                for record_id in component
                if record_id in previous_cluster_by_record
            }
        )
        cluster_id = next((candidate for candidate in candidate_cluster_ids if candidate not in used_cluster_ids), None)
        if cluster_id is None:
            cluster_id = f"C-{next_cluster_number:05d}"
            next_cluster_number += 1
        used_cluster_ids.add(cluster_id)
        for record_id in component:
            assigned_cluster_ids[record_id] = cluster_id
    return assigned_cluster_ids


def _allocate_golden_rows(
    *,
    clustered_rows: list[dict[str, str]],
    impacted_cluster_ids: set[str],
    previous_golden_by_cluster: dict[str, dict[str, str]],
    source_priority: tuple[str, ...],
    field_rules: dict[str, str],
    reused_golden_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    used_golden_ids = {row.get("golden_id", "") for row in reused_golden_rows if row.get("golden_id")}
    previous_golden_ids = {row.get("golden_id", "") for row in previous_golden_by_cluster.values() if row.get("golden_id")}
    next_golden_number = _max_identifier_number(previous_golden_ids | used_golden_ids, "G") + 1

    rows_by_cluster: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in clustered_rows:
        rows_by_cluster[str(row.get("cluster_id", "")).strip()].append(row)

    recalculated_golden_rows: list[dict[str, str]] = []
    for cluster_id in sorted(impacted_cluster_ids):
        previous_golden = previous_golden_by_cluster.get(cluster_id)
        golden_id = None
        if previous_golden is not None:
            previous_golden_id = str(previous_golden.get("golden_id", "")).strip()
            if previous_golden_id and previous_golden_id not in used_golden_ids:
                golden_id = previous_golden_id
        if golden_id is None:
            golden_id = f"G-{next_golden_number:05d}"
            next_golden_number += 1
        used_golden_ids.add(golden_id)
        recalculated_golden_rows.append(
            merge_records(
                rows_by_cluster[cluster_id],
                golden_id=golden_id,
                source_priority=source_priority,
                field_rules=field_rules,
            )
        )

    return sorted(reused_golden_rows + recalculated_golden_rows, key=lambda row: str(row.get("cluster_id", "")))


def refresh_incremental_run(
    *,
    current_rows: list[dict[str, str]],
    previous_bundle: PersistedRunBundle,
    config: PipelineConfig,
) -> IncrementalRefreshResult:
    current_rows_by_id = {_source_record_id(row): dict(row) for row in current_rows if _source_record_id(row)}
    previous_rows_by_id = {_source_record_id(row): dict(row) for row in previous_bundle.normalized_rows if _source_record_id(row)}
    previous_cluster_by_record, previous_cluster_members, previous_cluster_rows_by_record = _build_previous_cluster_indexes(previous_bundle)
    review_overrides = build_review_override_map(previous_bundle.review_rows)
    current_record_ids = set(current_rows_by_id)
    previous_match_decisions = {
        review_pair_key(str(row.get("left_id", "")), str(row.get("right_id", ""))): str(row.get("decision", ""))
        for row in previous_bundle.candidate_pairs
    }
    review_override_pairs = {
        pair for pair in review_overrides if pair[0] in current_record_ids and pair[1] in current_record_ids
    }
    effective_review_override_pairs = {
        pair
        for pair in review_override_pairs
        if previous_match_decisions.get(pair) != _override_decision(review_overrides[pair])
    }

    impacted_record_ids, diff_counts = _collect_impacted_record_ids(
        current_rows_by_id=current_rows_by_id,
        previous_rows_by_id=previous_rows_by_id,
        previous_cluster_by_record=previous_cluster_by_record,
        previous_cluster_members=previous_cluster_members,
        review_override_pairs=effective_review_override_pairs,
        config=config,
    )
    reused_record_ids = current_record_ids - impacted_record_ids

    if not impacted_record_ids:
        match_rows = sorted(
            [
                dict(row)
                for row in previous_bundle.candidate_pairs
                if set(_pair_key(str(row.get("left_id", "")), str(row.get("right_id", "")))) <= current_record_ids
            ],
            key=lambda row: (str(row.get("left_id", "")), str(row.get("right_id", ""))),
        )
        clustered_rows = [
            {**current_rows_by_id[record_id], "cluster_id": previous_cluster_by_record[record_id]}
            for record_id in sorted(current_record_ids)
            if record_id in previous_cluster_by_record
        ]
        cluster_output_rows = [
            {
                "cluster_id": row.get("cluster_id", ""),
                "source_record_id": row.get("source_record_id", ""),
                "source_system": row.get("source_system", ""),
                "person_entity_id": row.get("person_entity_id", ""),
            }
            for row in clustered_rows
        ]
        active_review_rows, review_rows = build_review_case_rows(
            match_rows,
            previous_review_cases=previous_bundle.review_rows,
        )
        golden_rows = sorted(
            [dict(row) for row in previous_bundle.golden_rows],
            key=lambda row: str(row.get("cluster_id", "")),
        )
        crosswalk_rows = _build_crosswalk_rows(clustered_rows, golden_rows)
        blocking_metrics_rows = [
            {
                **dict(row),
                "overall_deduplicated_candidate_pair_count": len(match_rows),
            }
            for row in previous_bundle.blocking_metrics_rows
        ]
        metadata = {
            "mode": "incremental",
            "fallback_to_full": False,
            "predecessor_run_id": previous_bundle.run.run_id,
            "affected_record_count": 0,
            "reused_record_count": len(reused_record_ids),
            "recalculated_candidate_pair_count": 0,
            "reused_candidate_pair_count": len(match_rows),
            "recalculated_cluster_count": 0,
            "reused_cluster_count": len({row.get("cluster_id", "") for row in cluster_output_rows}),
            **diff_counts,
        }
        return IncrementalRefreshResult(
            match_rows=match_rows,
            blocking_metrics_rows=blocking_metrics_rows,
            clustered_rows=clustered_rows,
            cluster_output_rows=cluster_output_rows,
            golden_rows=golden_rows,
            crosswalk_rows=crosswalk_rows,
            active_review_rows=active_review_rows,
            review_rows=review_rows,
            metadata=metadata,
        )

    impacted_rows = [current_rows_by_id[record_id] for record_id in sorted(impacted_record_ids)]
    recalculated_review_override_pairs = {
        pair
        for pair in review_override_pairs
        if pair[0] in impacted_record_ids and pair[1] in impacted_record_ids
    }
    recalculated_match_rows, recalculated_blocking_metrics = _build_match_rows(
        impacted_rows,
        config,
        forced_pairs=recalculated_review_override_pairs,
        review_overrides=review_overrides,
    )
    reused_match_rows = [
        dict(row)
        for row in previous_bundle.candidate_pairs
        if {
            str(row.get("left_id", "")).strip(),
            str(row.get("right_id", "")).strip(),
        } <= reused_record_ids
    ]
    match_rows = sorted(
        reused_match_rows + recalculated_match_rows,
        key=lambda row: (str(row.get("left_id", "")), str(row.get("right_id", ""))),
    )
    blocking_metrics_rows = _blocking_metrics_rows(
        recalculated_blocking_metrics,
        overall_candidate_pair_count=len(match_rows),
    )

    impacted_components = _component_record_ids(impacted_record_ids, recalculated_match_rows)
    reused_cluster_rows = [
        dict(previous_cluster_rows_by_record[record_id])
        for record_id in sorted(reused_record_ids)
        if record_id in previous_cluster_rows_by_record
    ]
    reused_cluster_ids = {row.get("cluster_id", "") for row in reused_cluster_rows if row.get("cluster_id")}
    impacted_cluster_assignments = _allocate_cluster_ids(
        impacted_components,
        previous_cluster_by_record=previous_cluster_by_record,
        reserved_cluster_ids=reused_cluster_ids,
        previous_cluster_ids=set(previous_cluster_members),
    )

    clustered_rows = []
    for record_id in sorted(current_record_ids):
        if record_id in impacted_cluster_assignments:
            cluster_id = impacted_cluster_assignments[record_id]
        else:
            cluster_id = previous_cluster_by_record[record_id]
        clustered_rows.append({**current_rows_by_id[record_id], "cluster_id": cluster_id})

    cluster_output_rows = [
        {
            "cluster_id": row.get("cluster_id", ""),
            "source_record_id": row.get("source_record_id", ""),
            "source_system": row.get("source_system", ""),
            "person_entity_id": row.get("person_entity_id", ""),
        }
        for row in clustered_rows
    ]

    impacted_cluster_ids = {impacted_cluster_assignments[record_id] for record_id in impacted_record_ids}
    previous_golden_by_cluster = {
        str(row.get("cluster_id", "")).strip(): dict(row)
        for row in previous_bundle.golden_rows
        if str(row.get("cluster_id", "")).strip()
    }
    reused_golden_rows = [
        dict(row)
        for row in previous_bundle.golden_rows
        if str(row.get("cluster_id", "")).strip() in reused_cluster_ids
    ]
    golden_rows = _allocate_golden_rows(
        clustered_rows=clustered_rows,
        impacted_cluster_ids=impacted_cluster_ids,
        previous_golden_by_cluster=previous_golden_by_cluster,
        source_priority=config.survivorship.source_priority,
        field_rules=config.survivorship.field_rules,
        reused_golden_rows=reused_golden_rows,
    )
    crosswalk_rows = _build_crosswalk_rows(clustered_rows, golden_rows)
    active_review_rows, review_rows = build_review_case_rows(
        match_rows,
        previous_review_cases=previous_bundle.review_rows,
    )

    metadata = {
        "mode": "incremental",
        "fallback_to_full": False,
        "predecessor_run_id": previous_bundle.run.run_id,
        "affected_record_count": len(impacted_record_ids),
        "reused_record_count": len(reused_record_ids),
        "recalculated_candidate_pair_count": len(recalculated_match_rows),
        "reused_candidate_pair_count": len(reused_match_rows),
        "recalculated_cluster_count": len(impacted_cluster_ids),
        "reused_cluster_count": len(reused_cluster_ids),
        **diff_counts,
    }
    return IncrementalRefreshResult(
        match_rows=match_rows,
        blocking_metrics_rows=blocking_metrics_rows,
        clustered_rows=clustered_rows,
        cluster_output_rows=cluster_output_rows,
        golden_rows=golden_rows,
        crosswalk_rows=crosswalk_rows,
        active_review_rows=active_review_rows,
        review_rows=review_rows,
        metadata=metadata,
    )
