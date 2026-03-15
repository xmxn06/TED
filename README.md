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
setx TED_API_KEY "YOUR_TED_API_KEY"
```

Open a new terminal after `setx` so the env var is visible.

## Run once

```powershell
python .\ted_ingest.py --max-pages 2 --limit 100 --query "publication-date>=today(-3)"
```

## Outputs

- Raw responses: `data/raw/*.json`
- Normalized notices: `data/parsed/*_normalized_notices.json`
- Scored notices: `data/scored/*_scored_notices.json`
- SQLite DB: `data/db/ted_notices.sqlite`
- Logs: `data/logs/ingestion.log`

## Phase 3 Scoring

Scoring uses deterministic filters first:

- target countries
- target CPV code prefixes
- keyword include/exclude lists
- contract value range
- deadline window

Configure these in `scoring_config.json`.

Run scoring:

```powershell
python .\ted_score.py --config .\scoring_config.json
```

Optional AI overlay for:

- short summary
- why relevant
- likely fit
- risks/blockers
- bid/no-bid rationale

```powershell
python .\ted_score.py --config .\scoring_config.json --enable-ai
```

`--enable-ai` uses `OPENAI_API_KEY` if present. Deterministic scoring always runs first.

## Phase 4 Daily Digest

Generate MVP ranked output (top 10 opportunities) from scored notices:

```powershell
python .\ted_digest.py --top-n 10
```

Digest output fields per opportunity:

- 3-line summary
- why it matched
- deadline
- value
- link

Output path:

- `data/digest/*_daily_ranked_digest.md`

## Daily pipeline trigger

Use Windows Task Scheduler to run:

```powershell
powershell.exe -ExecutionPolicy Bypass -File "C:\Users\Aman\ted_pipeline\run_daily.ps1"
```

That keeps ingestion running daily before any UI work.
