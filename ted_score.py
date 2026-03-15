import argparse
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv


AI_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


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
    hits: List[str] = []
    lowered = text.lower()
    for kw in keywords:
        kw_clean = kw.strip().lower()
        if not kw_clean:
            continue
        if " " in kw_clean:
            if kw_clean in lowered:
                hits.append(kw)
            continue
        pattern = r"\b" + re.escape(kw_clean) + r"\b"
        if re.search(pattern, lowered):
            hits.append(kw)
    return hits


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


def any_deadline_in_window(deadlines: List[Any], min_days: int, max_days: int) -> bool:
    for deadline in deadlines:
        if deadline_in_window(parse_datetime_maybe(deadline), min_days, max_days):
            return True
    return False


def any_value_in_range(values: List[Any], minimum: Optional[float], maximum: Optional[float]) -> bool:
    for val in values:
        try:
            numeric = float(val)
        except (TypeError, ValueError):
            continue
        if value_in_range(numeric, minimum, maximum):
            return True
    return False


def deterministic_score(notice: Dict[str, Any], config: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    weights = config.get("weights", {})
    target_countries = [c.upper() for c in config.get("target_countries", [])]
    target_cpv_prefixes = [str(x) for x in config.get("target_cpv_prefixes", [])]
    hard_include_keywords = config.get("keywords", {}).get("hard_include", [])
    soft_include_keywords = config.get("keywords", {}).get("soft_include", [])
    exclude_keywords = config.get("keywords", {}).get("exclude", [])
    hard_required = bool(config.get("keywords", {}).get("hard_required", False))
    soft_match_ratio = float(config.get("keywords", {}).get("soft_match_ratio", 0.65))
    value_range = config.get("value_range_eur", {})
    deadline_window = config.get("deadline_window_days", {"min": 0, "max": 365})
    value_neutral_on_missing = bool(config.get("value_neutral_on_missing", True))
    lot_scoring = config.get("lot_scoring", {})
    lot_enabled = bool(lot_scoring.get("enabled", True))

    title = notice.get("title") or ""
    description = notice.get("description") or ""
    text_blob = f"{title}\n{description}"
    notice_cpv_codes = notice.get("cpv_codes") or []
    lot_cpv_codes = notice.get("lot_cpv_codes") or []
    cpv_codes = notice_cpv_codes + lot_cpv_codes if lot_enabled else notice_cpv_codes
    country = (notice.get("country") or "").upper()
    value = notice.get("estimated_value_eur")
    lot_values = notice.get("lot_values_eur") or []
    deadline_dt = parse_datetime_maybe(notice.get("deadline_at"))
    lot_deadlines = notice.get("lot_deadlines") or []

    hard_hits = keyword_hits(hard_include_keywords, text_blob)
    soft_hits = keyword_hits(soft_include_keywords, text_blob)
    excludes_hit = keyword_hits(exclude_keywords, text_blob)

    country_ok = not target_countries or country in target_countries
    cpv_ok = cpv_matches(target_cpv_prefixes, cpv_codes)
    any_keyword_lists = bool(hard_include_keywords or soft_include_keywords)
    if excludes_hit:
        keywords_ok = False
        keyword_ratio = 0.0
    elif not any_keyword_lists:
        keywords_ok = True
        keyword_ratio = 1.0
    elif hard_hits:
        keywords_ok = True
        keyword_ratio = 1.0
    elif hard_required:
        keywords_ok = False
        keyword_ratio = 0.0
    elif soft_hits:
        keywords_ok = True
        keyword_ratio = soft_match_ratio
    else:
        keywords_ok = False
        keyword_ratio = 0.0
    value_ok_notice = value_in_range(value, value_range.get("min"), value_range.get("max"))
    value_ok_lot = any_value_in_range(lot_values, value_range.get("min"), value_range.get("max")) if lot_enabled else False
    value_ok = value_ok_notice or value_ok_lot
    value_missing = (value is None) and (len(lot_values) == 0)

    deadline_ok_notice = deadline_in_window(deadline_dt, deadline_window.get("min", 0), deadline_window.get("max", 365))
    deadline_ok_lot = (
        any_deadline_in_window(lot_deadlines, deadline_window.get("min", 0), deadline_window.get("max", 365))
        if lot_enabled
        else False
    )
    deadline_ok = deadline_ok_notice or deadline_ok_lot

    country_w = float(weights.get("country", 20.0))
    cpv_w = float(weights.get("cpv", 25.0))
    keywords_w = float(weights.get("keywords", 25.0))
    value_w = float(weights.get("value", 15.0))
    deadline_w = float(weights.get("deadline", 15.0))

    applicable_weights = {
        "country": country_w,
        "cpv": cpv_w,
        "keywords": keywords_w,
        "value": 0.0 if (value_missing and value_neutral_on_missing) else value_w,
        "deadline": deadline_w,
    }
    earned_weights = {
        "country": country_w if country_ok else 0.0,
        "cpv": cpv_w if cpv_ok else 0.0,
        "keywords": keywords_w * keyword_ratio,
        "value": value_w if value_ok else 0.0,
        "deadline": deadline_w if deadline_ok else 0.0,
    }
    applicable_total = sum(applicable_weights.values())
    earned_total = sum(earned_weights.values())
    notice_score_total = round((earned_total / applicable_total) * 100.0, 2) if applicable_total > 0 else 0.0

    # Best-lot score isolates lot-level commercial quality to avoid over-rewarding noisy multi-lot notices.
    best_lot_score = 0.0
    if lot_enabled:
        lot_count = max(len(lot_deadlines), len(lot_values), len(lot_cpv_codes))
        for idx in range(lot_count):
            lot_cpv = [lot_cpv_codes[idx]] if idx < len(lot_cpv_codes) else []
            lot_value = lot_values[idx] if idx < len(lot_values) else None
            lot_deadline = lot_deadlines[idx] if idx < len(lot_deadlines) else None
            lot_cpv_ok = cpv_matches(target_cpv_prefixes, lot_cpv)
            lot_value_ok = value_in_range(lot_value, value_range.get("min"), value_range.get("max"))
            lot_deadline_ok = deadline_in_window(
                parse_datetime_maybe(lot_deadline), deadline_window.get("min", 0), deadline_window.get("max", 365)
            )
            lot_earned = (cpv_w if lot_cpv_ok else 0.0) + (value_w if lot_value_ok else 0.0) + (deadline_w if lot_deadline_ok else 0.0)
            lot_applicable = cpv_w + value_w + deadline_w
            if lot_applicable > 0:
                lot_score = round((lot_earned / lot_applicable) * 100.0, 2)
                if lot_score > best_lot_score:
                    best_lot_score = lot_score

    aggregation = config.get("score_aggregation", {})
    aggregation_method = str(aggregation.get("method", "blended")).lower()
    notice_weight = float(aggregation.get("notice_weight", 0.7))
    lot_weight = float(aggregation.get("lot_weight", 0.3))
    if aggregation_method == "max":
        score_total = max(notice_score_total, best_lot_score)
    else:
        weight_sum = notice_weight + lot_weight
        if weight_sum <= 0:
            score_total = notice_score_total
        else:
            score_total = round((notice_score_total * notice_weight + best_lot_score * lot_weight) / weight_sum, 2)

    details = {
        "filters": {
            "country_ok": country_ok,
            "cpv_ok": cpv_ok,
            "keywords_ok": keywords_ok,
            "value_ok": value_ok,
            "deadline_ok": deadline_ok,
            "value_neutral_on_missing": value_missing and value_neutral_on_missing,
            "value_ok_notice": value_ok_notice,
            "value_ok_lot": value_ok_lot,
            "deadline_ok_notice": deadline_ok_notice,
            "deadline_ok_lot": deadline_ok_lot,
            "hard_required": hard_required,
        },
        "evidence": {
            "country": country,
            "cpv_codes": cpv_codes,
            "notice_cpv_codes": notice_cpv_codes,
            "lot_cpv_codes": lot_cpv_codes,
            "hard_keywords_hit": hard_hits,
            "soft_keywords_hit": soft_hits,
            "exclude_keywords_hit": excludes_hit,
            "estimated_value_eur": value,
            "lot_values_eur": lot_values,
            "deadline_at": notice.get("deadline_at"),
            "lot_deadlines": lot_deadlines,
        },
        "score_breakdown": {
            "weights": applicable_weights,
            "earned": earned_weights,
            "applicable_total": applicable_total,
            "earned_total": earned_total,
            "keyword_ratio": keyword_ratio,
            "notice_score_total": notice_score_total,
            "best_lot_score": best_lot_score,
            "total": score_total,
        },
    }
    return score_total, details


def ai_empty(status: str) -> Dict[str, Any]:
    return {
        "short_summary": None,
        "why_relevant": None,
        "likely_fit": None,
        "risks_blockers": None,
        "bid_no_bid_rationale": None,
        "ai_status": status,
    }


def validate_ai_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    required_keys = [
        "short_summary",
        "why_relevant",
        "likely_fit",
        "risks_blockers",
        "bid_no_bid_rationale",
    ]
    normalized = {key: raw.get(key) for key in required_keys}
    normalized["ai_status"] = raw.get("ai_status", "ok")
    return normalized


def call_openai_structured(
    prompt: str, model: str, max_tokens: int, max_retries: int = 3, backoff_seconds: float = 1.5
) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return ai_empty("skipped_missing_openai_api_key")

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
        "max_tokens": max_tokens,
    }

    last_error: Optional[str] = None
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=60,
            )
        except requests.RequestException as exc:
            last_error = type(exc).__name__
            if attempt < max_retries:
                time.sleep(backoff_seconds * (2**attempt))
                continue
            return ai_empty(f"failed: {last_error}")

        if response.status_code in AI_RETRYABLE_STATUS and attempt < max_retries:
            time.sleep(backoff_seconds * (2**attempt))
            continue
        if response.status_code >= 400:
            return ai_empty(f"failed_http_{response.status_code}")

        try:
            content = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
        except Exception as exc:
            last_error = type(exc).__name__
            if attempt < max_retries:
                time.sleep(backoff_seconds * (2**attempt))
                continue
            return ai_empty(f"failed: {last_error}")

        parsed["ai_status"] = "ok"
        return validate_ai_payload(parsed)

    return ai_empty(f"failed: {last_error or 'unknown'}")


def enrich_with_ai(
    notice: Dict[str, Any],
    details: Dict[str, Any],
    model: str,
    ai_max_description_chars: int,
    ai_max_tokens: int,
    cache: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    cache_key = str(notice.get("notice_id") or "")
    if cache_key and cache_key in cache:
        cached = dict(cache[cache_key])
        cached["ai_status"] = cached.get("ai_status", "ok_cached")
        return validate_ai_payload(cached)

    compact_description = (notice.get("description") or "")[:ai_max_description_chars]
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
            "description": compact_description,
            "procedure_type": notice.get("procedure_type"),
            "deterministic_score": details.get("score_breakdown", {}).get("total"),
            "deterministic_filters": details.get("filters"),
        },
        ensure_ascii=False,
    )
    try:
        ai_result = call_openai_structured(prompt, model=model, max_tokens=ai_max_tokens)
        if cache_key:
            cache[cache_key] = ai_result
        return ai_result
    except Exception as exc:  # Keep pipeline resilient.
        return ai_empty(f"failed: {type(exc).__name__}")


def run_scoring(
    notices: List[Dict[str, Any]],
    config: Dict[str, Any],
    enable_ai: bool,
    ai_model: str,
    ai_max_notices: int,
    ai_max_description_chars: int,
    ai_max_tokens: int,
    ai_cache_path: Path,
) -> List[Dict[str, Any]]:
    threshold = float(config.get("decision_threshold", 60.0))
    output: List[Dict[str, Any]] = []
    ai_cache: Dict[str, Dict[str, Any]] = {}
    if ai_cache_path.exists():
        try:
            loaded = load_json(ai_cache_path)
            if isinstance(loaded, dict):
                ai_cache = loaded
        except Exception:
            ai_cache = {}
    ai_calls = 0

    for notice in notices:
        score, details = deterministic_score(notice, config)
        breakdown = details.get("score_breakdown", {})
        record = {
            **notice,
            "relevance_score": score,
            "notice_score": breakdown.get("notice_score_total", score),
            "best_lot_score": breakdown.get("best_lot_score", 0.0),
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
            if ai_calls < ai_max_notices:
                record["ai"] = enrich_with_ai(
                    notice,
                    details,
                    model=ai_model,
                    ai_max_description_chars=ai_max_description_chars,
                    ai_max_tokens=ai_max_tokens,
                    cache=ai_cache,
                )
                ai_calls += 1
            else:
                record["ai"] = ai_empty("skipped_ai_budget_limit")
        output.append(record)
    if enable_ai:
        ai_cache_path.parent.mkdir(parents=True, exist_ok=True)
        ai_cache_path.write_text(json.dumps(ai_cache, ensure_ascii=False, indent=2), encoding="utf-8")
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
        required=True,
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
    parser.add_argument("--ai-max-notices", type=int, default=25, help="Maximum notices to enrich with AI.")
    parser.add_argument(
        "--ai-max-description-chars",
        type=int,
        default=1200,
        help="Max description chars sent to AI per notice.",
    )
    parser.add_argument("--ai-max-tokens", type=int, default=350, help="Max response tokens for AI enrichment.")
    parser.add_argument(
        "--ai-cache-path",
        default="data/scored/ai_cache.json",
        help="Path to AI enrichment cache file.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
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
        ai_max_notices=args.ai_max_notices,
        ai_max_description_chars=args.ai_max_description_chars,
        ai_max_tokens=args.ai_max_tokens,
        ai_cache_path=Path(args.ai_cache_path).resolve(),
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
