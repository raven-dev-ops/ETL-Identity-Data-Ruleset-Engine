from etl_identity_engine.matching.blocking import generate_candidates
from etl_identity_engine.matching.scoring import score_pair


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

