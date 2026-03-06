param(
  [Parameter(Mandatory = $true)]
  [string]$BaseUrl,
  [Parameter(Mandatory = $true)]
  [string]$OpsApiKey,
  [string]$BundleOutput = "dataforge_bundle.zip"
)

$ErrorActionPreference = "Stop"

function Write-Step([string]$Message) {
  Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Invoke-Json([string]$Method, [string]$Url, [hashtable]$Headers = @{}) {
  $resp = Invoke-WebRequest -Method $Method -Uri $Url -Headers $Headers -UseBasicParsing
  if ([string]::IsNullOrWhiteSpace($resp.Content)) {
    return $null
  }
  return ($resp.Content | ConvertFrom-Json)
}

$base = $BaseUrl.TrimEnd("/")

Write-Step "Health check"
$health = Invoke-Json "GET" "$base/health"
if ($health.status -ne "ok") {
  throw "Health check failed."
}
Write-Host "Health: ok"

Write-Step "Unauthorized ops check"
try {
  Invoke-WebRequest -Method GET -Uri "$base/ops/runtime" -UseBasicParsing | Out-Null
  throw "Expected unauthorized response for /ops/runtime without x-api-key."
} catch {
  $status = $_.Exception.Response.StatusCode.value__
  if ($status -ne 401) {
    throw "Expected 401, got $status."
  }
  Write-Host "Unauthorized check passed (401)."
}

Write-Step "Authorized runtime check"
$headers = @{ "x-api-key" = $OpsApiKey }
$runtime = Invoke-Json "GET" "$base/ops/runtime" $headers
Write-Host ("Runtime: mode={0}, db={1}" -f $runtime.runtime_mode, $runtime.db_path)

Write-Step "Observed pipeline run"
$pipeline = Invoke-Json "POST" "$base/ops/pipeline/run?auto_accept_inference=true" $headers
if (-not $pipeline.correlation_id) {
  throw "Missing correlation_id from pipeline run."
}
$correlationId = $pipeline.correlation_id
Write-Host "Correlation ID: $correlationId"

Write-Step "Download artifact bundle"
$bundleUrl = "$base/exports/run/$correlationId.zip"
Invoke-WebRequest -Method GET -Uri $bundleUrl -OutFile $BundleOutput -UseBasicParsing
if (-not (Test-Path $BundleOutput)) {
  throw "Bundle file not created."
}
Write-Host "Bundle downloaded: $BundleOutput"

Write-Step "Verification complete"
Write-Host "All deployment checks passed."
