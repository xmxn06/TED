import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ted_ingest import build_expert_query


def test_build_expert_query_cpv_only() -> None:
    cfg = {
        "window_days": 21,
        "cpv_prefixes": ["356", "355", "3581"],
        "title_or_buyer_include": [],
        "exclude_keywords": [],
    }
    q = build_expert_query(cfg)
    assert "publication-date>=today(-21)" in q
    assert "classification-cpv=356*" in q
    assert "classification-cpv=355*" in q
    assert "classification-cpv=3581*" in q
    assert "NOT" not in q


def test_build_expert_query_with_excludes() -> None:
    cfg = {
        "window_days": 14,
        "cpv_prefixes": ["356"],
        "exclude_keywords": ["medical", "uniform"],
    }
    q = build_expert_query(cfg)
    assert "NOT notice-title~medical" in q
    assert "NOT notice-title~uniform" in q


def test_build_expert_query_with_include_terms() -> None:
    cfg = {
        "window_days": 7,
        "cpv_prefixes": [],
        "title_or_buyer_include": ["radar", "surveillance"],
        "exclude_keywords": [],
    }
    q = build_expert_query(cfg)
    assert "notice-title~radar" in q
    assert "buyer-name~radar" in q
    assert "notice-title~surveillance" in q


def test_build_expert_query_empty_lists() -> None:
    cfg = {
        "window_days": 30,
        "cpv_prefixes": [],
        "title_or_buyer_include": [],
        "exclude_keywords": [],
    }
    q = build_expert_query(cfg)
    assert q == "publication-date>=today(-30)"


def test_build_expert_query_full_config() -> None:
    cfg = {
        "window_days": 21,
        "cpv_prefixes": ["356", "355"],
        "title_or_buyer_include": ["drone"],
        "exclude_keywords": ["hospital"],
    }
    q = build_expert_query(cfg)
    assert "publication-date>=today(-21)" in q
    assert "classification-cpv=356*" in q
    assert "notice-title~drone" in q
    assert "NOT notice-title~hospital" in q
    assert q.count("AND") >= 3
