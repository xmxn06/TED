import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ted_ingest import normalize_country_code, normalize_notice, parse_estimated_value_eur


def test_country_normalization_alpha2_to_alpha3() -> None:
    assert normalize_country_code("de") == "DEU"
    assert normalize_country_code(["fr"]) == "FRA"
    assert normalize_country_code("POL") == "POL"


def test_parse_estimated_value_eur_respects_currency() -> None:
    assert parse_estimated_value_eur({"amount": "120000", "currency": "EUR"}) == 120000.0
    assert parse_estimated_value_eur({"amount": "120000", "currency": "USD"}) is None
    assert parse_estimated_value_eur("500000") == 500000.0


def test_notice_normalization_includes_lot_fields() -> None:
    raw_notice = {
        "publication-number": "123-2026",
        "notice-title": {"eng": "Radar maintenance framework"},
        "description-proc": {"eng": "Maintenance for airborne radar systems"},
        "buyer-name": {"eng": ["Ministry of Defence"]},
        "buyer-country": ["DE"],
        "publication-date": "2026-03-15+01:00",
        "deadline-date-lot": ["2026-04-05+01:00", "2026-04-07+01:00"],
        "classification-cpv": ["50660000"],
        "estimated-value-glo": {"amount": "2500000", "currency": "EUR"},
        "estimated-value-lot": [{"amount": "1200000", "currency": "EUR"}],
        "procedure-type": "open",
        "links": {"htmlDirect": {"ENG": "https://ted.europa.eu/en/notice/123-2026/html"}},
    }

    normalized = normalize_notice(raw_notice)

    assert normalized["source"] == "TED"
    assert normalized["notice_id"] == "123-2026"
    assert normalized["country"] == "DEU"
    assert normalized["estimated_value_eur"] == 2500000.0
    assert normalized["lots_count"] == 2
    assert normalized["lot_deadlines"] == ["2026-04-05+01:00", "2026-04-07+01:00"]
    assert normalized["lot_values_eur"] == [1200000.0]
