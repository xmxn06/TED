# TED Notices Pipeline

This pipeline follows a staged build:

1. Pull recent notices from TED (`POST /v3/notices/search`)
2. Store raw JSON payloads
3. Normalize into a clean internal schema for ranking
4. Score relevance with deterministic filters
5. Optionally layer AI reasoning fields on top

## Parsed fields (first pass only)

- notice ID
- title
- description/summary
- buyer name
- country
- publication date
- deadline
- CPV codes
- estimated value (if present)
- procedure type
- URL

## Setup

```powershell
cd C:\Users\Aman\ted_pipeline
python -m pip install -r requirements.txt
copy .env.example .env
# edit .env and set TED_API_KEY (and OPENAI_API_KEY if needed)
```

The scripts auto-load `.env` via `python-dotenv`.

## TED Auth Header Mode

`ted_ingest.py` uses one auth style at a time (no redundant dual header):

- default: `X-API-KEY: <key>`
- optional: `Authorization: ApiKey <key>`

Switch mode with:

```powershell
python .\ted_ingest.py --auth-header Authorization
```

## Run once (ICP-driven)

```powershell
python .\ted_ingest.py --retrieval-config .\configs\retrieval_defense_hardware_sensors.json --max-pages 3 --limit 100
python .\ted_score.py --config .\configs\scoring_defense_hardware_sensors.json
python .\ted_digest.py --top-n 10
```

Manual query override (for ad-hoc exploration only):

```powershell
python .\ted_ingest.py --query "classification-cpv=356* AND publication-date>=today(-7)" --max-pages 1 --limit 50
```

## Outputs

- Raw responses: `data/raw/*.json`
- Normalized notices: `data/parsed/*_normalized_notices.json`
- Scored notices: `data/scored/*_scored_notices.json`
- SQLite DB: `data/db/ted_notices.sqlite`
- Logs: `data/logs/ingestion.log`

## Normalized Schema (sample)

```json
{
  "source": "TED",
  "notice_id": "170861-2026",
  "title": "...",
  "buyer": "...",
  "country": "IRL",
  "published_at": "2026-03-12+01:00",
  "deadline_at": null,
  "cpv_codes": ["42997300"],
  "estimated_value_eur": null,
  "description": "...",
  "procedure_type": "open",
  "url": "https://ted.europa.eu/en/notice/170861-2026/html",
  "lots_count": 0,
  "lot_deadlines": [],
  "lot_cpv_codes": [],
  "lot_values_eur": [],
  "raw_payload": {}
}
```

## Phase 3 Scoring

Scoring uses deterministic filters first:

- target countries
- target CPV code prefixes
- keyword include/exclude lists
- contract value range
- deadline window

Select an ICP profile explicitly on every scoring run (no implicit default):

- `configs/scoring_defense_hardware_sensors.json`
- `configs/scoring_aerospace_mro_avionics.json`
- `configs/scoring_defense_software_secure_systems.json`

`scoring_config.json` can be used as your active local tuning copy after picking one profile.

The keyword block supports:

- `hard_include`: high-confidence signals (boosts strongly)
- `soft_include`: broad relevance signals (partial boost)
- `exclude`: disqualifying terms
- `hard_required`: optional strict mode (default `false`)

Run scoring:

```powershell
python .\ted_score.py --config .\configs\scoring_defense_hardware_sensors.json
```

Optional AI overlay for:

- short summary
- why relevant
- likely fit
- risks/blockers
- bid/no-bid rationale

```powershell
python .\ted_score.py --config .\configs\scoring_defense_hardware_sensors.json --enable-ai
```

`--enable-ai` uses `OPENAI_API_KEY` if present. Deterministic scoring always runs first.
AI safeguards included:

- compact prompt payload (truncated description)
- response shape validation
- retry/backoff for transient AI API failures (`429/5xx` and request errors)
- max-notice budget (`--ai-max-notices`)
- max token budget (`--ai-max-tokens`)
- local enrichment cache (`data/scored/ai_cache.json`)

Score aggregation behavior is configurable:

- `score_aggregation.method`: `blended` (default) or `max`
- `notice_weight` and `lot_weight` apply when `blended` is used

## Phase 4 Daily Digest

Generate MVP ranked output (top 10 opportunities) from scored notices:

```powershell
python .\ted_digest.py --top-n 10
```

Digest output fields per opportunity:

- 3-line summary
- why it matched (signal-based relevance rationale)
- scoring view (`notice_score` vs `best_lot_score`)
- recommendation (bid/no-bid)
- confidence
- top blocker
- next step
- deadline
- value
- link

Output path:

- `data/digest/*_daily_ranked_digest.md`

## Tests

Run parser and scoring tests:

```powershell
pytest -q
```

## Evaluation Workflow (Calibration Set)

Create a manual labeling set from current ranked output:

```powershell
python .\ted_eval.py init --limit 200
```

Fill `label` column in CSV (`1` relevant, `0` irrelevant), then run:

```powershell
python .\ted_eval.py evaluate --labels .\data\eval\labels_template.csv --top-k 10
python .\ted_eval.py evaluate --labels .\data\eval\labels_template.csv --top-k 20
```

Reports include `precision_at_k`, `recall_at_k`, pool size, avg notice/lot scores, and lot-dominant count.

Suggest exclude keywords from observed false positives:

```powershell
python .\ted_eval.py suggest-excludes --labels .\data\eval\labels_template.csv
```

## Expected Folder Tree After First Run

```text
ted_pipeline/
  configs/
    retrieval_defense_hardware_sensors.json
    scoring_defense_hardware_sensors.json
    scoring_aerospace_mro_avionics.json
    scoring_defense_software_secure_systems.json
  tests/
    test_ingest.py
    test_score.py
    test_retrieval.py
  data/
    db/ted_notices.sqlite
    digest/*_daily_ranked_digest.md
    eval/*_evaluation_report.json
    logs/ingestion.log
    parsed/*_normalized_notices.json
    raw/*_page_*.json
    scored/*_scored_notices.json
  ted_ingest.py
  ted_score.py
  ted_digest.py
  ted_eval.py
```

## Failure Modes and Handling

- `429/5xx` from TED: automatic exponential retries.
- network timeout: retried and counted in `timeouts` metric.
- parser field drift: `raw_payload` preserved in every normalized row.
- AI enrichment failure: deterministic score still emitted; `ai_status` captures the error.
- missing value: treated as neutral (not a penalty) by default via scoring config.

## Reset / Clean Rerun

```powershell
Remove-Item -Recurse -Force .\data\

python .\ted_ingest.py --retrieval-config .\configs\retrieval_defense_hardware_sensors.json --max-pages 3 --limit 100
python .\ted_score.py --config .\configs\scoring_defense_hardware_sensors.json
python .\ted_digest.py --top-n 10
```

## Daily pipeline trigger

Use Windows Task Scheduler to run:

```powershell
powershell.exe -ExecutionPolicy Bypass -File "C:\Users\Aman\ted_pipeline\run_daily.ps1"
```

That keeps ingestion running daily before any UI work.
