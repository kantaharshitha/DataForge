$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r .\backend\requirements.txt
python .\backend\run_migrations.py

Write-Host 'Setup complete. Start API: uvicorn backend.app.main:app --reload'
Write-Host 'Start UI: streamlit run ui/app.py'
