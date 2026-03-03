$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
Set-Location $root

if (-not (Test-Path .\.venv\Scripts\python.exe)) {
    throw "Virtual environment not found at .venv. Run setup_phase1.ps1 first."
}

$py = ".\.venv\Scripts\python.exe"

Write-Host "Running migrations..."
& $py .\backend\run_migrations.py

Write-Host "Running test suite..."
& $py -m pytest .\tests -q

Write-Host "All checks passed."
