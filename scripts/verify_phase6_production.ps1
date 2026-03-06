param(
  [Parameter(Mandatory = $true)]
  [string]$BaseUrl,
  [Parameter(Mandatory = $true)]
  [string]$OpsApiKey,
  [string]$EvidenceFile = "phase6_production_verification.json"
)

$ErrorActionPreference = "Stop"

function Invoke-Json([string]$Method, [string]$Url, [hashtable]$Headers = @{}, [string]$Body = "") {
  if ([string]::IsNullOrWhiteSpace($Body)) {
    $resp = Invoke-WebRequest -Method $Method -Uri $Url -Headers $Headers -UseBasicParsing
  } else {
    $resp = Invoke-WebRequest -Method $Method -Uri $Url -Headers $Headers -Body $Body -ContentType "application/json" -UseBasicParsing
  }
  if ([string]::IsNullOrWhiteSpace($resp.Content)) {
    return $null
  }
  return ($resp.Content | ConvertFrom-Json)
}

$base = $BaseUrl.TrimEnd("/")
$headers = @{ "x-api-key" = $OpsApiKey }

$evidence = [ordered]@{
  executed_at_utc = (Get-Date).ToUniversalTime().ToString("o")
  base_url = $base
  checks = @()
}

Write-Host "Running Phase 6 production verification against $base"

$health = Invoke-Json "GET" "$base/health"
$evidence.checks += @{ check = "health"; status = "pass"; payload = $health }

$runtime = Invoke-Json "GET" "$base/ops/runtime" $headers
$evidence.checks += @{ check = "ops_runtime_authorized"; status = "pass"; payload = $runtime }

$beforeSummary = Invoke-Json "GET" "$base/alerts/summary?window_hours=24"
$evidence.checks += @{ check = "alerts_summary_before"; status = "pass"; payload = $beforeSummary }

$escalation = Invoke-Json "POST" "$base/ops/alerts/escalate/run?older_than_minutes=1&limit=200" $headers
$evidence.checks += @{ check = "escalation_scan"; status = "pass"; payload = $escalation }

$recentAlerts = Invoke-Json "GET" "$base/alerts/recent?limit=50"
$deliveredCount = @($recentAlerts | Where-Object { $_.delivery_status -eq "DELIVERED" }).Count
$hasEscalated = @($recentAlerts | Where-Object { $_.alert_type -eq "ALERT_ESCALATED" }).Count -gt 0
$evidence.checks += @{
  check = "alerts_recent_post_scan"
  status = "pass"
  delivered_count = $deliveredCount
  has_escalated_event = $hasEscalated
}

$afterSummary = Invoke-Json "GET" "$base/alerts/summary?window_hours=24"
$evidence.checks += @{ check = "alerts_summary_after"; status = "pass"; payload = $afterSummary }

$json = $evidence | ConvertTo-Json -Depth 8
Set-Content -Path $EvidenceFile -Value $json -Encoding UTF8
Write-Host "Verification complete. Evidence written to $EvidenceFile"
