import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set


STOPWORDS: Set[str] = {
    "the", "a", "an", "and", "or", "of", "for", "to", "in", "on", "at", "by",
    "is", "it", "de", "du", "des", "la", "le", "les", "et", "en", "un", "une",
    "das", "der", "die", "und", "von", "für", "im", "den", "dem", "zur",
    "w", "z", "i", "na", "do", "od", "za", "se", "si", "pro", "ve", "ze",
}


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def latest_scored_file(scored_dir: Path) -> Path:
    files = sorted(scored_dir.glob("*_scored_notices.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No scored files found in: {scored_dir}")
    return files[0]


def write_label_template(scored: List[Dict[str, Any]], out_csv: Path, limit: int) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    ranked = sorted(scored, key=lambda x: float(x.get("relevance_score", 0.0)), reverse=True)[:limit]

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "notice_id",
                "label",
                "notes",
                "relevance_score",
                "notice_score",
                "best_lot_score",
                "title",
                "country",
                "buyer",
                "link",
            ],
        )
        writer.writeheader()
        for row in ranked:
            writer.writerow(
                {
                    "notice_id": row.get("notice_id"),
                    "label": "",
                    "notes": "",
                    "relevance_score": row.get("relevance_score"),
                    "notice_score": row.get("notice_score"),
                    "best_lot_score": row.get("best_lot_score"),
                    "title": row.get("title"),
                    "country": row.get("country"),
                    "buyer": row.get("buyer"),
                    "link": row.get("url") or "",
                }
            )


def parse_labels(path: Path) -> Dict[str, int]:
    labels: Dict[str, int] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            notice_id = (row.get("notice_id") or "").strip()
            label_raw = (row.get("label") or "").strip().lower()
            if not notice_id:
                continue
            if label_raw in {"1", "relevant", "yes", "y", "true"}:
                labels[notice_id] = 1
            elif label_raw in {"0", "irrelevant", "no", "n", "false"}:
                labels[notice_id] = 0
    return labels


def evaluate(scored: List[Dict[str, Any]], labels: Dict[str, int], top_k: int) -> Dict[str, Any]:
    ranked = sorted(scored, key=lambda x: float(x.get("relevance_score", 0.0)), reverse=True)
    labeled_ranked = [row for row in ranked if str(row.get("notice_id")) in labels]
    if not labeled_ranked:
        return {
            "pool_size": len(ranked),
            "labeled_count": 0,
            "precision_at_k": None,
            "recall_at_k": None,
            "coverage": 0.0,
        }

    top_slice = labeled_ranked[:top_k]
    tp_top = sum(labels[str(row.get("notice_id"))] for row in top_slice)
    relevant_total = sum(labels.values())
    precision_at_k = tp_top / len(top_slice) if top_slice else 0.0
    recall_at_k = tp_top / relevant_total if relevant_total > 0 else 0.0
    coverage = len(labeled_ranked) / len(ranked) if ranked else 0.0

    notice_scores = [float(row.get("notice_score", 0)) for row in labeled_ranked]
    lot_scores = [float(row.get("best_lot_score", 0)) for row in labeled_ranked]
    lot_dominant = sum(1 for n, l in zip(notice_scores, lot_scores) if l > n)

    return {
        "pool_size": len(ranked),
        "labeled_count": len(labels),
        "precision_at_k": round(precision_at_k, 4),
        "recall_at_k": round(recall_at_k, 4),
        "coverage": round(coverage, 4),
        "relevant_total": relevant_total,
        "top_k": top_k,
        "avg_notice_score": round(sum(notice_scores) / len(notice_scores), 2) if notice_scores else 0.0,
        "avg_best_lot_score": round(sum(lot_scores) / len(lot_scores), 2) if lot_scores else 0.0,
        "lot_dominant_count": lot_dominant,
    }


def tokenize(text: str) -> List[str]:
    return [w for w in re.findall(r"[a-zA-ZÀ-ÿ]{3,}", text.lower()) if w not in STOPWORDS]


def suggest_excludes(
    scored: List[Dict[str, Any]], labels: Dict[str, int], top_n_terms: int = 20
) -> List[Dict[str, Any]]:
    fp_tokens: Counter[str] = Counter()
    tp_tokens: Counter[str] = Counter()

    for row in scored:
        nid = str(row.get("notice_id"))
        if nid not in labels:
            continue
        text = f"{row.get('title', '')} {row.get('description', '')}"
        tokens = tokenize(text)
        if labels[nid] == 0:
            fp_tokens.update(tokens)
        else:
            tp_tokens.update(tokens)

    suggestions: List[Dict[str, Any]] = []
    for token, fp_count in fp_tokens.most_common(200):
        tp_count = tp_tokens.get(token, 0)
        if fp_count > tp_count and fp_count >= 2:
            suggestions.append({"term": token, "fp_count": fp_count, "tp_count": tp_count, "diff": fp_count - tp_count})
    suggestions.sort(key=lambda x: x["diff"], reverse=True)
    return suggestions[:top_n_terms]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create, evaluate, and diagnose labeled relevance sets.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_cmd = sub.add_parser("init", help="Generate CSV label template from scored notices.")
    init_cmd.add_argument("--input", default="", help="Scored JSON input. Uses latest if omitted.")
    init_cmd.add_argument("--output", default="data/eval/labels_template.csv", help="Output CSV for manual labeling.")
    init_cmd.add_argument("--limit", type=int, default=200, help="How many top notices to include in template.")

    eval_cmd = sub.add_parser("evaluate", help="Compute evaluation metrics from manual labels.")
    eval_cmd.add_argument("--input", default="", help="Scored JSON input. Uses latest if omitted.")
    eval_cmd.add_argument("--labels", default="data/eval/labels_template.csv", help="CSV with manual labels.")
    eval_cmd.add_argument("--top-k", type=int, default=20, help="Precision/recall cutoff.")
    eval_cmd.add_argument("--output", default="data/eval", help="Folder for evaluation report JSON.")

    suggest_cmd = sub.add_parser("suggest-excludes", help="Suggest exclude keywords from false-positive patterns.")
    suggest_cmd.add_argument("--input", default="", help="Scored JSON input.")
    suggest_cmd.add_argument("--labels", required=True, help="CSV with manual labels.")
    suggest_cmd.add_argument("--top-n", type=int, default=20, help="How many candidate terms to suggest.")
    return parser.parse_args()


def resolve_input(raw: str) -> Path:
    if raw:
        return Path(raw).resolve()
    return latest_scored_file(Path("data/scored").resolve())


def main() -> int:
    args = parse_args()
    input_path = resolve_input(getattr(args, "input", ""))
    scored = load_json(input_path)
    if not isinstance(scored, list):
        raise ValueError("Scored notices JSON must contain a list.")

    if args.command == "init":
        out_csv = Path(args.output).resolve()
        write_label_template(scored, out_csv=out_csv, limit=args.limit)
        print(f"Scored input: {input_path}")
        print(f"Label template: {out_csv}")
        print(f"Rows written: {min(args.limit, len(scored))}")
        return 0

    if args.command == "suggest-excludes":
        labels_path = Path(args.labels).resolve()
        labels = parse_labels(labels_path)
        suggestions = suggest_excludes(scored, labels=labels, top_n_terms=args.top_n)
        print(json.dumps(suggestions, indent=2))
        return 0

    labels_path = Path(args.labels).resolve()
    labels = parse_labels(labels_path)
    report = evaluate(scored, labels=labels, top_k=args.top_k)
    report["scored_input"] = str(input_path)
    report["labels_file"] = str(labels_path)
    report["generated_at"] = datetime.now(timezone.utc).isoformat()

    out_dir = Path(args.output).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_evaluation_report.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))
    print(f"Evaluation report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
