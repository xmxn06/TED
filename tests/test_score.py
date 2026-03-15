from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ted_score import deterministic_score, keyword_hits, run_scoring


def iso_in_days(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()


def base_config() -> dict:
    return {
        "target_countries": ["DEU"],
        "target_cpv_prefixes": ["356", "5066"],
        "keywords": {
            "hard_include": ["radar"],
            "soft_include": ["uav", "avionics"],
            "exclude": ["catering"],
            "hard_required": False,
            "soft_match_ratio": 0.65,
        },
        "value_range_eur": {"min": 100000, "max": 10000000},
        "deadline_window_days": {"min": 0, "max": 60},
        "value_neutral_on_missing": True,
        "lot_scoring": {"enabled": True},
        "score_aggregation": {"method": "blended", "notice_weight": 0.7, "lot_weight": 0.3},
        "weights": {"country": 20, "cpv": 30, "keywords": 30, "value": 10, "deadline": 10},
        "decision_threshold": 60,
    }


def test_keyword_hits_phrase_and_word_boundaries() -> None:
    text = "This covers radar systems and secure communications for UAV platforms."
    hits = keyword_hits(["radar", "secure communications", "air"], text)
    assert "radar" in hits
    assert "secure communications" in hits
    assert "air" not in hits  # should not match as substring in unrelated words


def test_deterministic_scoring_missing_value_is_neutral() -> None:
    cfg = base_config()
    notice = {
        "country": "DEU",
        "title": "Radar support",
        "description": "Radar avionics support for UAV fleet",
        "cpv_codes": ["50660000"],
        "lot_cpv_codes": [],
        "estimated_value_eur": None,
        "lot_values_eur": [],
        "deadline_at": iso_in_days(20),
        "lot_deadlines": [],
    }
    score, details = deterministic_score(notice, cfg)
    assert score > 0
    assert details["filters"]["value_neutral_on_missing"] is True
    assert details["score_breakdown"]["weights"]["value"] == 0.0


def test_deterministic_scoring_deadline_outside_window() -> None:
    cfg = base_config()
    notice = {
        "country": "DEU",
        "title": "Radar package",
        "description": "Radar systems upgrade",
        "cpv_codes": ["35600000"],
        "lot_cpv_codes": [],
        "estimated_value_eur": 500000,
        "lot_values_eur": [],
        "deadline_at": iso_in_days(120),
        "lot_deadlines": [],
    }
    score, details = deterministic_score(notice, cfg)
    assert details["filters"]["deadline_ok"] is False
    assert score < 100


def test_lot_level_fields_affect_scoring() -> None:
    cfg = base_config()
    notice = {
        "country": "DEU",
        "title": "General services framework",
        "description": "Program support contract",
        "cpv_codes": ["72000000"],  # notice-level non-match
        "lot_cpv_codes": ["35613000"],  # lot-level match
        "estimated_value_eur": None,
        "lot_values_eur": [750000],
        "deadline_at": None,
        "lot_deadlines": [iso_in_days(25)],
    }
    score, details = deterministic_score(notice, cfg)
    assert details["filters"]["cpv_ok"] is True
    assert details["filters"]["value_ok_lot"] is True
    assert details["filters"]["deadline_ok_lot"] is True
    assert score > 0


def test_hard_keyword_optional_can_still_score_with_soft_hit() -> None:
    cfg = base_config()
    cfg["keywords"]["hard_include"] = ["electronic warfare"]
    cfg["keywords"]["soft_include"] = ["uav"]
    cfg["keywords"]["hard_required"] = False
    notice = {
        "country": "DEU",
        "title": "UAV support contract",
        "description": "Avionics and uav integration services",
        "cpv_codes": ["35613000"],
        "lot_cpv_codes": [],
        "estimated_value_eur": 200000,
        "lot_values_eur": [],
        "deadline_at": iso_in_days(10),
        "lot_deadlines": [],
    }
    score, details = deterministic_score(notice, cfg)
    assert details["filters"]["keywords_ok"] is True
    assert details["score_breakdown"]["keyword_ratio"] == 0.65
    assert score > 0


def test_exclude_keyword_overrides_keyword_match() -> None:
    cfg = base_config()
    notice = {
        "country": "DEU",
        "title": "Radar and avionics catering support",
        "description": "Includes radar maintenance but also catering services",
        "cpv_codes": ["35613000"],
        "lot_cpv_codes": [],
        "estimated_value_eur": 300000,
        "lot_values_eur": [],
        "deadline_at": iso_in_days(20),
        "lot_deadlines": [],
    }
    score, details = deterministic_score(notice, cfg)
    assert details["filters"]["keywords_ok"] is False
    assert "catering" in details["evidence"]["exclude_keywords_hit"]
    assert details["score_breakdown"]["keyword_ratio"] == 0.0
    assert score < 100


def test_scored_record_exposes_notice_and_best_lot_scores(tmp_path: Path) -> None:
    cfg = base_config()
    notice = {
        "country": "DEU",
        "title": "General services framework",
        "description": "Program support contract with uav mention",
        "cpv_codes": ["72000000"],
        "lot_cpv_codes": ["35613000"],
        "estimated_value_eur": None,
        "lot_values_eur": [750000],
        "deadline_at": None,
        "lot_deadlines": [iso_in_days(25)],
    }
    scored = run_scoring(
        notices=[notice],
        config=cfg,
        enable_ai=False,
        ai_model="gpt-4o-mini",
        ai_max_notices=0,
        ai_max_description_chars=200,
        ai_max_tokens=100,
        ai_cache_path=tmp_path / "ai_cache.json",
    )
    assert len(scored) == 1
    row = scored[0]
    assert "notice_score" in row
    assert "best_lot_score" in row
    assert row["relevance_score"] >= 0
