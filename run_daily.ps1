$ErrorActionPreference = "Stop"

# Expects TED_API_KEY to be available in environment variables.
# Example one-time setup:
#   setx TED_API_KEY "your_key_here"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

python .\ted_ingest.py --max-pages 3 --limit 100 --query "publication-date>=today(-3)"
