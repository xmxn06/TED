import argparse
import json
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests


DEFAULT_BASE_URL = "https://api.ted.europa.eu/v3"
DEFAULT_LIMIT = 100
DEFAULT_MAX_PAGES = 1
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_QUERY = "publication-date>=today(-1)"

TED_FIELDS = [
    "publication-number",
    "notice-title",
    "description-proc",
    "buyer-name",
    "buyer-country",
    "publication-date",
    "deadline-date-lot",
    "classification-cpv",
    "estimated-value-glo",
    "procedure-type",
    "links",
]


@dataclass
class Metrics:
    request_count: int = 0
    response_bytes: int = 0
    failure_count: int = 0
    notices_seen: int = 0
    notices_parsed: int = 0


def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "ingestion.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def get_nested(data: Dict[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def first_non_empty(data: Dict[str, Any], candidates: Iterable[str]) -> Any:
    for key in candidates:
        value = get_nested(data, key) if "." in key else data.get(key)
        if value is not None and value != "":
            return value
    return None


def normalize_cpv_codes(raw_value: Any) -> List[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, str):
        return [raw_value]
    if isinstance(raw_value, list):
        normalized: List[str] = []
        for item in raw_value:
            if isinstance(item, str):
                normalized.append(item)
            elif isinstance(item, dict):
                code = first_non_empty(item, ["code", "id", "value", "cpvCode"])
                if code:
                    normalized.append(str(code))
        return normalized
    if isinstance(raw_value, dict):
        maybe_list = raw_value.get("codes")
        if isinstance(maybe_list, list):
            return normalize_cpv_codes(maybe_list)
    return []


def parse_estimated_value_eur(raw_value: Any) -> Optional[float]:
    if raw_value is None:
        return None
    if isinstance(raw_value, (int, float)):
        return float(raw_value)
    if isinstance(raw_value, str):
        cleaned = raw_value.replace(",", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return None
    if isinstance(raw_value, dict):
        for key in ["amount", "value", "estimated", "eur", "EUR"]:
            maybe_value = raw_value.get(key)
            parsed = parse_estimated_value_eur(maybe_value)
            if parsed is not None:
                return parsed
    if isinstance(raw_value, list):
        for item in raw_value:
            parsed = parse_estimated_value_eur(item)
            if parsed is not None:
                return parsed
    return None


def extract_text(value: Any, preferred_lang: str = "eng") -> Any:
    if isinstance(value, dict):
        preferred = value.get(preferred_lang)
        if isinstance(preferred, list) and preferred:
            return preferred[0]
        if isinstance(preferred, str):
            return preferred

        for inner in value.values():
            if isinstance(inner, list) and inner:
                return inner[0]
            if isinstance(inner, str):
                return inner
    return value


def extract_notice_url(links_value: Any, preferred_lang: str = "ENG") -> Any:
    if not isinstance(links_value, dict):
        return None

    for section in ["htmlDirect", "html", "pdf", "xml"]:
        section_value = links_value.get(section)
        if not isinstance(section_value, dict):
            continue
        preferred = section_value.get(preferred_lang)
        if isinstance(preferred, str):
            return preferred
        for maybe_url in section_value.values():
            if isinstance(maybe_url, str):
                return maybe_url
    return None


def parse_notice(notice: Dict[str, Any]) -> Dict[str, Any]:
    cpv_source = first_non_empty(
        notice,
        [
            "classification-cpv",
            "cpvCodes",
            "cpv_codes",
            "cpv",
            "mainCpvCode",
            "classification.cpv",
            "classification.cpvCodes",
        ],
    )

    parsed = {
        "notice_id": first_non_empty(
            notice,
            [
                "publication-number",
                "noticeId",
                "notice-id",
                "id",
                "publicationNumber",
            ],
        ),
        "title": extract_text(
            first_non_empty(
                notice,
                ["notice-title", "title", "noticeTitle", "summary.title"],
            )
        ),
        "description": extract_text(
            first_non_empty(
                notice,
                ["description-proc", "description", "summary", "noticeDescription", "shortDescription"],
            )
        ),
        "buyer_name": extract_text(
            first_non_empty(
                notice,
                [
                    "buyer-name",
                    "buyerName",
                    "buyer.name",
                    "contractingAuthority.name",
                    "organisation.name",
                ],
            )
        ),
        "country": first_non_empty(
            notice,
            [
                "buyer-country",
                "country",
                "buyerCountry",
                "buyer.country",
                "placeOfPerformance.country",
            ],
        ),
        "publication_date": first_non_empty(
            notice,
            [
                "publicationDate",
                "publication-date",
                "publishedAt",
                "datePublished",
            ],
        ),
        "deadline": first_non_empty(
            notice,
            [
                "deadline-date-lot",
                "deadline",
                "submissionDeadline",
                "tenderDeadline",
                "responseDeadline",
            ],
        ),
        "cpv_codes": normalize_cpv_codes(cpv_source),
        "estimated_value": first_non_empty(
            notice,
            [
                "estimated-value-glo",
                "estimatedValue",
                "estimated-value",
                "contract.estimatedValue",
                "value.estimated",
            ],
        ),
        "procedure_type": first_non_empty(
            notice,
            ["procedure-type", "procedureType", "procedure.type"],
        ),
        "url": extract_notice_url(first_non_empty(notice, ["links"])),
    }

    if isinstance(parsed["country"], list):
        parsed["country"] = parsed["country"][0] if parsed["country"] else None
    if isinstance(parsed["deadline"], list):
        parsed["deadline"] = parsed["deadline"][0] if parsed["deadline"] else None
    if isinstance(parsed["estimated_value"], list):
        parsed["estimated_value"] = parsed["estimated_value"][0] if parsed["estimated_value"] else None

    return parsed


def normalize_notice(notice: Dict[str, Any]) -> Dict[str, Any]:
    parsed = parse_notice(notice)
    return {
        "source": "TED",
        "notice_id": parsed["notice_id"],
        "title": parsed["title"],
        "buyer": parsed["buyer_name"],
        "country": parsed["country"],
        "published_at": parsed["publication_date"],
        "deadline_at": parsed["deadline"],
        "cpv_codes": parsed["cpv_codes"],
        "estimated_value_eur": parse_estimated_value_eur(parsed["estimated_value"]),
        "description": parsed["description"],
        "procedure_type": parsed["procedure_type"],
        "raw_payload": notice,
    }


def extract_notices(response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = ["notices", "results", "items", "content", "data"]
    for key in candidates:
        value = response_json.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]

    if isinstance(response_json, list):
        return [x for x in response_json if isinstance(x, dict)]
    return []


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_notices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            page INTEGER NOT NULL,
            fetched_at TEXT NOT NULL,
            http_status INTEGER NOT NULL,
            response_bytes INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS normalized_notices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            source TEXT NOT NULL,
            notice_id TEXT,
            title TEXT,
            buyer TEXT,
            country TEXT,
            published_at TEXT,
            deadline_at TEXT,
            cpv_codes_json TEXT,
            estimated_value_eur REAL,
            description TEXT,
            procedure_type TEXT,
            raw_payload_json TEXT NOT NULL
        );
        """
    )
    conn.commit()


def make_headers(api_key: str) -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-API-KEY": api_key,
        "Authorization": f"ApiKey {api_key}",
    }


def run_ingestion(
    api_key: str,
    output_dir: Path,
    base_url: str,
    query: str,
    limit: int,
    max_pages: int,
    timeout_seconds: int,
) -> Metrics:
    metrics = Metrics()

    raw_dir = output_dir / "raw"
    parsed_dir = output_dir / "parsed"
    log_dir = output_dir / "logs"
    db_dir = output_dir / "db"

    raw_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)
    db_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(log_dir)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    db_path = db_dir / "ted_notices.sqlite"
    conn = sqlite3.connect(db_path)
    init_db(conn)

    normalized_batch: List[Dict[str, Any]] = []
    endpoint = f"{base_url.rstrip('/')}/notices/search"
    headers = make_headers(api_key)

    try:
        for page in range(1, max_pages + 1):
            payload = {
                "query": query,
                "fields": TED_FIELDS,
                "limit": limit,
                "page": page,
                "paginationMode": "PAGE_NUMBER",
            }
            metrics.request_count += 1

            try:
                response = requests.post(
                    endpoint,
                    headers=headers,
                    json=payload,
                    timeout=timeout_seconds,
                )
            except requests.RequestException as exc:
                metrics.failure_count += 1
                logging.exception("Request failed for page %s: %s", page, exc)
                continue

            raw_text = response.text
            raw_size = len(raw_text.encode("utf-8"))
            metrics.response_bytes += raw_size

            fetched_at = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """
                INSERT INTO raw_notices (run_id, page, fetched_at, http_status, response_bytes, payload_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, page, fetched_at, response.status_code, raw_size, raw_text),
            )
            conn.commit()

            raw_file = raw_dir / f"{run_id}_page_{page}.json"
            raw_file.write_text(raw_text, encoding="utf-8")

            if response.status_code >= 400:
                metrics.failure_count += 1
                logging.error(
                    "Page %s failed with status %s (bytes=%s).",
                    page,
                    response.status_code,
                    raw_size,
                )
                continue

            try:
                response_json = response.json()
            except json.JSONDecodeError:
                metrics.failure_count += 1
                logging.error("Page %s returned non-JSON response.", page)
                continue

            notices = extract_notices(response_json)
            if not notices:
                logging.info("No notices found on page %s; stopping pagination.", page)
                break

            metrics.notices_seen += len(notices)
            for notice in notices:
                normalized = normalize_notice(notice)
                normalized_batch.append(normalized)
                metrics.notices_parsed += 1

                conn.execute(
                    """
                    INSERT INTO normalized_notices (
                        run_id, ingested_at, source, notice_id, title, buyer, country,
                        published_at, deadline_at, cpv_codes_json, estimated_value_eur, description,
                        procedure_type, raw_payload_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        fetched_at,
                        normalized["source"],
                        normalized["notice_id"],
                        normalized["title"],
                        normalized["buyer"],
                        normalized["country"],
                        normalized["published_at"],
                        normalized["deadline_at"],
                        json.dumps(normalized["cpv_codes"], ensure_ascii=False),
                        normalized["estimated_value_eur"],
                        normalized["description"],
                        normalized["procedure_type"],
                        json.dumps(normalized["raw_payload"], ensure_ascii=False),
                    ),
                )
            conn.commit()

            logging.info(
                "Fetched page %s: notices=%s, status=%s, bytes=%s",
                page,
                len(notices),
                response.status_code,
                raw_size,
            )

        parsed_path = parsed_dir / f"{run_id}_normalized_notices.json"
        parsed_path.write_text(
            json.dumps(normalized_batch, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        logging.info("Run complete | requests=%s", metrics.request_count)
        logging.info("Run complete | response_bytes=%s", metrics.response_bytes)
        logging.info("Run complete | failures=%s", metrics.failure_count)
        logging.info("Run complete | notices_seen=%s", metrics.notices_seen)
        logging.info("Run complete | notices_parsed=%s", metrics.notices_parsed)
        logging.info("SQLite DB: %s", db_path)
        logging.info("Raw payload folder: %s", raw_dir)
        logging.info("Parsed output file: %s", parsed_path)
        return metrics
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 1 TED ingestion: pull notices, store raw JSON, and parse ranking fields."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("TED_API_KEY"),
        help="TED API key. Defaults to TED_API_KEY environment variable.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="TED API base URL.")
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Directory where raw payloads, parsed outputs, logs, and SQLite DB are stored.",
    )
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help="TED expert query string. Default fetches notices published in last 1 day.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Notices per page (max 250).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help="How many pages to ingest in one run.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP request timeout in seconds.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print("Missing API key. Provide --api-key or set TED_API_KEY.", file=sys.stderr)
        return 2

    output_dir = Path(args.output_dir).resolve()
    run_ingestion(
        api_key=args.api_key,
        output_dir=output_dir,
        base_url=args.base_url,
        query=args.query,
        limit=args.limit,
        max_pages=args.max_pages,
        timeout_seconds=args.timeout_seconds,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
