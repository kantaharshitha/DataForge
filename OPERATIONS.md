# DataForge Operations Runbook

## Scope
This runbook covers deployment/runtime checks, protected ops endpoints, retention automation, and basic incident checks.

## Required Environment Variables
- `DATAFORGE_RUNTIME_MODE`
  - Recommended on Vercel: `vercel-ephemeral`
- `DATAFORGE_OPS_API_KEY`
  - Required if you want `/ops/*` endpoints protected

## Vercel Setup
1. Open Vercel Project Settings -> Environment Variables.
2. Add:
   - `DATAFORGE_RUNTIME_MODE=vercel-ephemeral`
   - `DATAFORGE_OPS_API_KEY=<strong-random-value>`
3. Redeploy the latest `main` deployment.

## Post-Deploy Verification
Use your deployed base URL (example: `https://your-app.vercel.app`).

1. Health check:
```bash
curl -i https://your-app.vercel.app/api/health
```

2. Ops endpoint should reject missing key:
```bash
curl -i https://your-app.vercel.app/api/ops/runtime
```
Expected: `401`

3. Ops endpoint with valid key:
```bash
curl -i -H "x-api-key: <DATAFORGE_OPS_API_KEY>" https://your-app.vercel.app/api/ops/runtime
```
Expected: `200`

4. Run observed pipeline:
```bash
curl -i -X POST -H "x-api-key: <DATAFORGE_OPS_API_KEY>" \
  "https://your-app.vercel.app/api/ops/pipeline/run?auto_accept_inference=true"
```
Capture `correlation_id` from response.

5. Download bundle:
```bash
curl -L -o dataforge_bundle.zip "https://your-app.vercel.app/api/exports/run/<correlation_id>.zip"
```

PowerShell automation (recommended):
```powershell
.\scripts\verify_deployment.ps1 -BaseUrl "https://your-app.vercel.app/api" -OpsApiKey "<DATAFORGE_OPS_API_KEY>"
```

## Retention Automation
- Workflow file: `.github/workflows/nightly_cleanup.yml`
- Required GitHub Actions secrets:
  - `DATAFORGE_BASE_URL` (for example `https://your-app.vercel.app/api`)
  - `DATAFORGE_OPS_API_KEY`
- Schedule: daily at 02:30 UTC

Manual trigger:
1. Open GitHub Actions.
2. Run workflow `Nightly Cleanup` (workflow_dispatch).

PowerShell API trigger (requires `GITHUB_TOKEN` env var):
```powershell
$env:GITHUB_TOKEN="<token-with-workflow-permission>"
.\scripts\trigger_cleanup_workflow.ps1 -Repo "kantaharshitha/DataForge" -Ref "main"
```

## Release and Evidence
1. Run production verification and save evidence:
```powershell
.\scripts\verify_phase6_production.ps1 -BaseUrl "https://your-app.vercel.app/api" -OpsApiKey "<DATAFORGE_OPS_API_KEY>" -EvidenceFile "phase6_production_verification.json"
```
2. Create GitHub release:
```powershell
$env:GITHUB_TOKEN="<token>"
.\scripts\create_github_release.ps1 -Repo "kantaharshitha/DataForge" -Tag "v1.5-alert-operations" -Title "DataForge v1.5-alert-operations" -NotesFile ".\RELEASE_NOTES_v1.5-alert-operations.md"
```

## Incident Checks
1. `500` on ops routes:
   - Validate `DATAFORGE_OPS_API_KEY` exists and request header is correct.
2. `401` unexpectedly:
   - Rotate/re-sync `DATAFORGE_OPS_API_KEY` in Vercel and Actions secrets.
3. Empty bundle export:
   - Confirm pipeline run completed and correlation ID is correct.
4. Data resets on Vercel:
   - Expected in `vercel-ephemeral` mode due to `/tmp` lifecycle.
