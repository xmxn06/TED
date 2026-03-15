import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


DEFAULT_CONFIG_PATH = "scoring_config.json"


@dataclass
class ScoreBreakdown:
    country: float = 0.0
    cpv: float = 0.0
    keywords: float = 0.0
    value: float = 0.0
    deadline: float = 0.0

    @property
    def total(self) -> float:
        return self.country + self.cpv + self.keywords + self.value + self.deadline


def parse_datetime_maybe(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None

    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    if len(cleaned) == 16 and (cleaned[-6] in {"+", "-"} and cleaned[-3] == ":"):
        cleaned = f"{cleaned[:10]}T00:00:00{cleaned[10:]}"
    elif len(cleaned) == 10:
        cleaned = f"{cleaned}T00:00:00+00:00"

    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def latest_normalized_file(parsed_dir: Path) -> Path:
    files = sorted(parsed_dir.glob("*_normalized_notices.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No normalized notice files found in: {parsed_dir}")
    return files[0]


def cpv_matches(target_prefixes: List[str], cpv_codes: List[str]) -> bool:
    if not target_prefixes:
        return True
    for cpv in cpv_codes:
        cpv_str = str(cpv)
        if any(cpv_str.startswith(prefix) for prefix in target_prefixes):
            return True
    return False


def keyword_hits(keywords: List[str], text: str) -> List[str]:
    lowered = text.lower()
    return [kw for kw in keywords if kw.lower() in lowered]


def value_in_range(value_eur: Optional[float], minimum: Optional[float], maximum: Optional[float]) -> bool:
    if value_eur is None:
        return False
    if minimum is not None and value_eur < minimum:
        return False
    if maximum is not None and value_eur > maximum:
        return False
    return True


def deadline_in_window(deadline: Optional[datetime], min_days: int, max_days: int) -> bool:
    if not deadline:
        return False
    now = datetime.now(timezone.utc)
    lower = now + timedelta(days=min_days)
    upper = now + timedelta(days=max_days)
    return lower <= deadline <= upper


def deterministic_score(notice: Dict[str, Any], config: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    weights = config.get("weights", {})
    target_countries = [c.upper() for c in config.get("target_countries", [])]
    target_cpv_prefixes = [str(x) for x in config.get("target_cpv_prefixes", [])]
    include_keywords = config.get("keywords", {}).get("include", [])
    exclude_keywords = config.get("keywords", {}).get("exclude", [])
    value_range = config.get("value_range_eur", {})
    deadline_window = config.get("deadline_window_days", {"min": 0, "max": 365})

    title = notice.get("title") or ""
    description = notice.get("description") or ""
    text_blob = f"{title}\n{description}"
    cpv_codes = notice.get("cpv_codes") or []
    country = (notice.get("country") or "").upper()
    value = notice.get("estimated_value_eur")
    deadline_dt = parse_datetime_maybe(notice.get("deadline_at"))

    includes_hit = keyword_hits(include_keywords, text_blob)
    excludes_hit = keyword_hits(exclude_keywords, text_blob)

    country_ok = not target_countries or country in target_countries
    cpv_ok = cpv_matches(target_cpv_prefixes, cpv_codes)
    keywords_ok = (len(include_keywords) == 0 or len(includes_hit) > 0) and len(excludes_hit) == 0
    value_ok = value_in_range(value, value_range.get("min"), value_range.get("max"))
    deadline_ok = deadline_in_window(deadline_dt, deadline_window.get("min", 0), deadline_window.get("max", 365))

    breakdown = ScoreBreakdown(
        country=weights.get("country", 20.0) if country_ok else 0.0,
        cpv=weights.get("cpv", 25.0) if cpv_ok else 0.0,
        keywords=weights.get("keywords", 25.0) if keywords_ok else 0.0,
        value=weights.get("value", 15.0) if value_ok else 0.0,
        deadline=weights.get("deadline", 15.0) if deadline_ok else 0.0,
    )

    details = {
        "filters": {
            "country_ok": country_ok,
            "cpv_ok": cpv_ok,
            "keywords_ok": keywords_ok,
            "value_ok": value_ok,
            "deadline_ok": deadline_ok,
        },
        "evidence": {
            "country": country,
            "cpv_codes": cpv_codes,
            "include_keywords_hit": includes_hit,
            "exclude_keywords_hit": excludes_hit,
            "estimated_value_eur": value,
            "deadline_at": notice.get("deadline_at"),
        },
        "score_breakdown": {
            "country": breakdown.country,
            "cpv": breakdown.cpv,
            "keywords": breakdown.keywords,
            "value": breakdown.value,
            "deadline": breakdown.deadline,
            "total": breakdown.total,
        },
    }
    return breakdown.total, details


def call_openai_structured(prompt: str, model: str) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {
            "short_summary": None,
            "why_relevant": None,
            "likely_fit": None,
            "risks_blockers": None,
            "bid_no_bid_rationale": None,
            "ai_status": "skipped_missing_openai_api_key",
        }

    payload = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "Return JSON with keys: short_summary, why_relevant, likely_fit, "
                    "risks_blockers, bid_no_bid_rationale. Keep concise."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    content = response.json()["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    parsed["ai_status"] = "ok"
    return parsed


def enrich_with_ai(notice: Dict[str, Any], details: Dict[str, Any], model: str) -> Dict[str, Any]:
    prompt = json.dumps(
        {
            "notice_id": notice.get("notice_id"),
            "title": notice.get("title"),
            "buyer": notice.get("buyer"),
            "country": notice.get("country"),
            "published_at": notice.get("published_at"),
            "deadline_at": notice.get("deadline_at"),
            "cpv_codes": notice.get("cpv_codes"),
            "estimated_value_eur": notice.get("estimated_value_eur"),
            "description": notice.get("description"),
            "procedure_type": notice.get("procedure_type"),
            "deterministic_score": details.get("score_breakdown", {}).get("total"),
            "deterministic_filters": details.get("filters"),
        },
        ensure_ascii=False,
    )
    try:
        return call_openai_structured(prompt, model=model)
    except Exception as exc:  # Keep pipeline resilient.
        return {
            "short_summary": None,
            "why_relevant": None,
            "likely_fit": None,
            "risks_blockers": None,
            "bid_no_bid_rationale": None,
            "ai_status": f"failed: {type(exc).__name__}",
        }


def run_scoring(
    notices: List[Dict[str, Any]],
    config: Dict[str, Any],
    enable_ai: bool,
    ai_model: str,
) -> List[Dict[str, Any]]:
    threshold = float(config.get("decision_threshold", 60.0))
    output: List[Dict[str, Any]] = []

    for notice in notices:
        score, details = deterministic_score(notice, config)
        record = {
            **notice,
            "relevance_score": score,
            "decision": "bid" if score >= threshold else "no_bid",
            "deterministic": details,
            "ai": {
                "short_summary": None,
                "why_relevant": None,
                "likely_fit": None,
                "risks_blockers": None,
                "bid_no_bid_rationale": None,
                "ai_status": "disabled",
            },
        }
        if enable_ai:
            record["ai"] = enrich_with_ai(notice, details, model=ai_model)
        output.append(record)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 3 scoring: deterministic ranking + optional AI enrichment."
    )
    parser.add_argument(
        "--input",
        default="",
        help="Normalized notices JSON file. If omitted, latest file in data/parsed is used.",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Scoring configuration JSON path.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/scored",
        help="Output directory for scored notices JSON.",
    )
    parser.add_argument(
        "--enable-ai",
        action="store_true",
        help="Enable optional AI enrichment fields.",
    )
    parser.add_argument(
        "--ai-model",
        default="gpt-4o-mini",
        help="Model for AI enrichment when --enable-ai is provided.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config).resolve()
    config = load_json(config_path)

    if args.input:
        input_path = Path(args.input).resolve()
    else:
        input_path = latest_normalized_file(Path("data/parsed").resolve())

    notices = load_json(input_path)
    if not isinstance(notices, list):
        raise ValueError("Input normalized notices JSON must contain a list.")

    scored = run_scoring(
        notices=notices,
        config=config,
        enable_ai=args.enable_ai,
        ai_model=args.ai_model,
    )

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = output_dir / f"{run_id}_scored_notices.json"
    output_path.write_text(json.dumps(scored, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Input: {input_path}")
    print(f"Config: {config_path}")
    print(f"Output: {output_path}")
    print(f"Scored notices: {len(scored)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
