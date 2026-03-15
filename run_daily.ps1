$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

python .\ted_ingest.py --retrieval-config .\configs\retrieval_defense_hardware_sensors.json --max-pages 3 --limit 100 --max-retries 3 --backoff-seconds 1.5 --auth-header X-API-KEY
python .\ted_score.py --config .\configs\scoring_defense_hardware_sensors.json
python .\ted_digest.py --top-n 10
