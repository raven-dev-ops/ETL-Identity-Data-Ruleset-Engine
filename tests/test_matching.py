from etl_identity_engine.matching.blocking import generate_candidates, generate_candidates_with_metrics
from etl_identity_engine.matching.clustering import assign_cluster_ids
from etl_identity_engine.matching.scoring import classify_score, explain_pair_score, score_pair


def test_generate_candidates_blocks_on_last_initial_and_dob() -> None:
    rows = [
        {"source_record_id": "1", "last_name": "SMITH", "canonical_dob": "1985-03-12"},
        {"source_record_id": "2", "last_name": "SMITH", "canonical_dob": "1985-03-12"},
        {"source_record_id": "3", "last_name": "JONES", "canonical_dob": "1985-03-12"},
    ]
    pairs = generate_candidates(rows)
    assert len(pairs) == 1


def test_score_pair_returns_higher_for_similar_records() -> None:
    left = {
        "canonical_name": "JOHN SMITH",
        "canonical_dob": "1985-03-12",
        "canonical_phone": "5551234567",
        "canonical_address": "123 MAIN STREET",
    }
    right = {
        "canonical_name": "JOHN SMITH",
        "canonical_dob": "1985-03-12",
        "canonical_phone": "5551234567",
        "canonical_address": "123 MAIN STREET",
    }
    assert score_pair(left, right) == 1.0


def test_score_pair_ignores_blank_matches() -> None:
    left = {
        "canonical_name": "",
        "canonical_dob": "",
        "canonical_phone": "",
        "canonical_address": "",
    }
    right = {
        "canonical_name": "",
        "canonical_dob": "",
        "canonical_phone": "",
        "canonical_address": "",
    }
    assert score_pair(left, right) == 0.0


def test_generate_candidates_uses_multiple_blocking_passes() -> None:
    rows = [
        {"source_record_id": "1", "last_name": "SMITH", "canonical_dob": "1985-03-12"},
        {"source_record_id": "2", "last_name": "SMITH", "canonical_dob": "1985-04-12"},
    ]

    pairs = generate_candidates(
        rows,
        blocking_passes=[("last_initial", "dob"), ("last_name", "birth_year")],
    )

    assert len(pairs) == 1


def test_generate_candidates_with_metrics_reports_raw_and_deduplicated_counts() -> None:
    rows = [
        {"source_record_id": "1", "last_name": "SMITH", "canonical_dob": "1985-03-12"},
        {"source_record_id": "2", "last_name": "SMITH", "canonical_dob": "1985-03-12"},
        {"source_record_id": "3", "last_name": "SMITH", "canonical_dob": "1985-04-12"},
    ]

    pairs, metrics = generate_candidates_with_metrics(
        rows,
        blocking_passes=[("last_initial", "dob"), ("last_name", "birth_year")],
        pass_names=["last_initial_plus_dob", "last_name_plus_birth_year"],
    )

    assert len(pairs) == 3
    assert metrics[0].pass_name == "last_initial_plus_dob"
    assert metrics[0].fields == ("last_initial", "dob")
    assert metrics[0].raw_candidate_pair_count == 1
    assert metrics[0].new_candidate_pair_count == 1
    assert metrics[0].cumulative_candidate_pair_count == 1
    assert metrics[1].pass_name == "last_name_plus_birth_year"
    assert metrics[1].raw_candidate_pair_count == 3
    assert metrics[1].new_candidate_pair_count == 2
    assert metrics[1].cumulative_candidate_pair_count == 3


def test_classify_score_uses_threshold_bands() -> None:
    assert classify_score(1.0, auto_merge=0.9, manual_review_min=0.6, no_match_max=0.59) == "auto_merge"
    assert classify_score(0.6, auto_merge=0.9, manual_review_min=0.6, no_match_max=0.59) == "manual_review"
    assert classify_score(0.2, auto_merge=0.9, manual_review_min=0.6, no_match_max=0.59) == "no_match"


def test_explain_pair_score_returns_reason_trace() -> None:
    left = {
        "canonical_name": "JOHN SMITH",
        "canonical_dob": "1985-03-12",
        "canonical_phone": "",
        "canonical_address": "123 MAIN STREET",
    }
    right = {
        "canonical_name": "JOHN SMITH",
        "canonical_dob": "1985-03-12",
        "canonical_phone": "5551234567",
        "canonical_address": "999 OAK AVENUE",
    }

    detail = explain_pair_score(left, right)

    assert detail.score == 0.8
    assert detail.matched_fields == ("canonical_name", "canonical_dob")
    assert detail.reason_trace == ("canonical_name:0.5", "canonical_dob:0.3")


def test_explain_pair_score_awards_partial_name_signal_for_same_person_variants() -> None:
    left = {
        "canonical_name": "JOHN SMITH",
        "canonical_dob": "1985-03-12",
        "canonical_phone": "",
        "canonical_address": "",
    }
    right = {
        "canonical_name": "J SMITH",
        "canonical_dob": "1985-03-12",
        "canonical_phone": "",
        "canonical_address": "",
    }

    detail = explain_pair_score(left, right)

    assert detail.score == 0.65
    assert detail.matched_fields == ("canonical_name_partial", "canonical_dob")
    assert detail.reason_trace == ("canonical_name_partial:0.35", "canonical_dob:0.3")


def test_partial_name_signal_does_not_auto_merge_without_other_supporting_signals() -> None:
    left = {
        "canonical_name": "JOHN SMITH",
        "canonical_dob": "",
        "canonical_phone": "",
        "canonical_address": "",
    }
    right = {
        "canonical_name": "JON SMITH",
        "canonical_dob": "",
        "canonical_phone": "",
        "canonical_address": "",
    }

    detail = explain_pair_score(left, right)

    assert detail.score == 0.35
    assert classify_score(
        detail.score,
        auto_merge=0.9,
        manual_review_min=0.6,
        no_match_max=0.59,
    ) == "no_match"


def test_assign_cluster_ids_is_deterministic_for_links_and_singletons() -> None:
    cluster_ids = assign_cluster_ids(
        ["A-1", "A-2", "B-1", "B-2"],
        [("A-2", "B-2"), ("A-1", "B-1")],
    )

    assert cluster_ids == {
        "A-1": "C-00001",
        "B-1": "C-00001",
        "A-2": "C-00002",
        "B-2": "C-00002",
    }

