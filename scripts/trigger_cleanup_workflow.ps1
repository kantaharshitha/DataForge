param(
  [Parameter(Mandatory = $true)]
  [string]$Repo, # owner/name
  [string]$Ref = "main",
  [string]$WorkflowFile = "nightly_cleanup.yml"
)

$ErrorActionPreference = "Stop"

if (-not $env:GITHUB_TOKEN) {
  throw "Set GITHUB_TOKEN env var with repo/workflow permissions."
}

$uri = "https://api.github.com/repos/$Repo/actions/workflows/$WorkflowFile/dispatches"
$headers = @{
  "Accept"        = "application/vnd.github+json"
  "Authorization" = "Bearer $($env:GITHUB_TOKEN)"
  "X-GitHub-Api-Version" = "2022-11-28"
}
$body = @{ ref = $Ref } | ConvertTo-Json

Invoke-WebRequest -Method POST -Uri $uri -Headers $headers -Body $body -ContentType "application/json" -UseBasicParsing | Out-Null
Write-Host "Triggered workflow '$WorkflowFile' on ref '$Ref' for $Repo"
