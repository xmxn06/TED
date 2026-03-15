import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def latest_scored_file(scored_dir: Path) -> Path:
    files = sorted(scored_dir.glob("*_scored_notices.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No scored files found in: {scored_dir}")
    return files[0]


def extract_notice_url(notice: Dict[str, Any]) -> Optional[str]:
    raw_payload = notice.get("raw_payload", {})
    links = raw_payload.get("links", {}) if isinstance(raw_payload, dict) else {}
    if not isinstance(links, dict):
        return None

    for section in ["htmlDirect", "html", "pdf", "xml"]:
        section_value = links.get(section)
        if not isinstance(section_value, dict):
            continue
        for preferred in ["ENG", "MUL", "eng"]:
            maybe = section_value.get(preferred)
            if isinstance(maybe, str):
                return maybe
        for maybe in section_value.values():
            if isinstance(maybe, str):
                return maybe
    return None


def truncate_text(text: str, max_len: int = 180) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= max_len:
        return cleaned
    return cleaned[: max_len - 3].rstrip() + "..."


def format_value(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        return f"EUR {float(value):,.0f}"
    except (TypeError, ValueError):
        return str(value)


def build_three_line_summary(notice: Dict[str, Any]) -> List[str]:
    title = truncate_text(notice.get("title") or "Untitled notice", max_len=180)
    buyer = notice.get("buyer") or "Unknown buyer"
    country = notice.get("country") or "N/A"
    procedure = notice.get("procedure_type") or "N/A"
    description = truncate_text(notice.get("description") or "No description provided.", max_len=180)

    line1 = title
    line2 = f"Buyer: {buyer} | Country: {country} | Procedure: {procedure}"
    line3 = description
    return [line1, line2, line3]


def build_why_matched(notice: Dict[str, Any]) -> str:
    deterministic = notice.get("deterministic", {})
    filters = deterministic.get("filters", {})
    evidence = deterministic.get("evidence", {})
    reasons: List[str] = []

    keyword_hits = (evidence.get("hard_keywords_hit") or []) + (evidence.get("soft_keywords_hit") or [])
    cpvs = evidence.get("cpv_codes") or []
    if filters.get("cpv_ok"):
        reasons.append(f"Technical scope aligns with target CPV families ({','.join(cpvs[:2]) if cpvs else 'matched'}).")
    if keyword_hits:
        reasons.append(f"Scope language indicates likely fit ({', '.join(keyword_hits[:3])}).")
    if filters.get("country_ok"):
        reasons.append(f"Buyer geography is in target coverage ({evidence.get('country', 'N/A')}).")
    if filters.get("value_ok"):
        lot_values = evidence.get("lot_values_eur") or []
        if filters.get("value_ok_lot") and lot_values:
            reasons.append(f"At least one lot is in commercial value range ({format_value(lot_values[0])}).")
        else:
            reasons.append(f"Contract value is inside target range ({format_value(evidence.get('estimated_value_eur'))}).")
    if filters.get("deadline_ok"):
        lot_deadlines = evidence.get("lot_deadlines") or []
        if filters.get("deadline_ok_lot") and lot_deadlines:
            reasons.append(f"Submission window appears actionable for at least one lot ({lot_deadlines[0]}).")
        else:
            reasons.append(f"Submission window appears actionable ({evidence.get('deadline_at')}).")

    if not reasons:
        return "Low-confidence fit; no strong commercial signal detected yet."
    return " ".join(reasons[:3])


def build_digest_markdown(scored: List[Dict[str, Any]], top_n: int) -> str:
    ranked = sorted(scored, key=lambda x: float(x.get("relevance_score", 0.0)), reverse=True)
    selected = ranked[:top_n]

    lines: List[str] = []
    lines.append(f"# TED Daily Ranked Digest ({datetime.now(timezone.utc).strftime('%Y-%m-%d')})")
    lines.append("")
    lines.append(f"Top {len(selected)} opportunities")
    lines.append("")

    for idx, notice in enumerate(selected, start=1):
        summary = build_three_line_summary(notice)
        why = build_why_matched(notice)
        deadline = notice.get("deadline_at") or "N/A"
        value = format_value(notice.get("estimated_value_eur"))
        link = extract_notice_url(notice) or "N/A"
        score = notice.get("relevance_score", 0)
        notice_score = notice.get("notice_score", score)
        best_lot_score = notice.get("best_lot_score", 0)
        decision = notice.get("decision", "no_bid")
        confidence = "high" if float(score) >= 75 else ("medium" if float(score) >= 55 else "low")
        blockers = []
        filters = notice.get("deterministic", {}).get("filters", {})
        if not filters.get("deadline_ok"):
            blockers.append("missing_or_out_of_window_deadline")
        if not filters.get("value_ok") and not filters.get("value_neutral_on_missing"):
            blockers.append("value_outside_target_range")
        if not filters.get("country_ok"):
            blockers.append("country_outside_target")
        top_blocker = blockers[0] if blockers else "none"
        next_step = (
            "Review tender docs and launch qualification checklist."
            if decision == "bid"
            else "Keep watchlisted and reassess if strategy changes."
        )

        lines.append(f"## {idx}. {notice.get('notice_id', 'unknown-id')} (score: {score})")
        lines.append("")
        lines.append("3-line summary:")
        lines.append(f"1) {summary[0]}")
        lines.append(f"2) {summary[1]}")
        lines.append(f"3) {summary[2]}")
        lines.append("")
        lines.append(f"Why it matched: {why}")
        lines.append(f"Scoring view: notice={notice_score}, best_lot={best_lot_score}")
        lines.append(f"Recommendation: {decision}")
        lines.append(f"Confidence: {confidence}")
        lines.append(f"Top blocker: {top_blocker}")
        lines.append(f"Next step: {next_step}")
        lines.append(f"Deadline: {deadline}")
        lines.append(f"Value: {value}")
        lines.append(f"Link: {link}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily ranked digest from scored notices.")
    parser.add_argument("--input", default="", help="Scored notices file; latest is used if omitted.")
    parser.add_argument("--output-dir", default="data/digest", help="Output directory for digest files.")
    parser.add_argument("--top-n", type=int, default=10, help="Number of top opportunities to include.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.input:
        input_path = Path(args.input).resolve()
    else:
        input_path = latest_scored_file(Path("data/scored").resolve())

    scored = load_json(input_path)
    if not isinstance(scored, list):
        raise ValueError("Scored notices JSON must contain a list.")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = output_dir / f"{run_id}_daily_ranked_digest.md"
    out_path.write_text(build_digest_markdown(scored, top_n=args.top_n), encoding="utf-8")

    print(f"Input: {input_path}")
    print(f"Output: {out_path}")
    print(f"Top requested: {args.top_n}")
    print(f"Total scored available: {len(scored)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
