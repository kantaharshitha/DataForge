param(
  [Parameter(Mandatory = $true)]
  [string]$Repo, # owner/name
  [Parameter(Mandatory = $true)]
  [string]$Tag,
  [Parameter(Mandatory = $true)]
  [string]$Title,
  [Parameter(Mandatory = $true)]
  [string]$NotesFile,
  [switch]$Draft,
  [switch]$Prerelease
)

$ErrorActionPreference = "Stop"

if (-not $env:GITHUB_TOKEN) {
  throw "Set GITHUB_TOKEN env var with repo scope."
}
if (-not (Test-Path $NotesFile)) {
  throw "Notes file not found: $NotesFile"
}

$notes = Get-Content $NotesFile -Raw
$uri = "https://api.github.com/repos/$Repo/releases"
$headers = @{
  "Accept" = "application/vnd.github+json"
  "Authorization" = "Bearer $($env:GITHUB_TOKEN)"
  "X-GitHub-Api-Version" = "2022-11-28"
}
$body = @{
  tag_name = $Tag
  name = $Title
  body = $notes
  draft = [bool]$Draft
  prerelease = [bool]$Prerelease
} | ConvertTo-Json

Invoke-WebRequest -Method POST -Uri $uri -Headers $headers -Body $body -ContentType "application/json" -UseBasicParsing | Out-Null
Write-Host "Created GitHub release '$Title' for tag '$Tag' in $Repo"
