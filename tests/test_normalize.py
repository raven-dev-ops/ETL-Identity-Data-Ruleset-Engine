from etl_identity_engine.normalize.addresses import normalize_address
from etl_identity_engine.normalize.dates import normalize_date
from etl_identity_engine.normalize.names import normalize_name
from etl_identity_engine.normalize.phones import normalize_phone


def test_normalize_name_handles_punctuation_and_case() -> None:
    assert normalize_name("Smith, John A.") == "JOHN A SMITH"


def test_normalize_date_parses_common_format() -> None:
    assert normalize_date("03/12/1985") == "1985-03-12"


def test_normalize_address_expands_suffix() -> None:
    assert normalize_address("123 Main St.") == "123 MAIN STREET"


def test_normalize_phone_strips_non_digits() -> None:
    assert normalize_phone("(555) 123-4567") == "5551234567"

