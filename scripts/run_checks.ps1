Param(
    [switch]$IncludeRemoteGitHubChecks,
    [string]$Repo = "raven-dev-ops/ETL-Identity-Data-Ruleset-Engine"
)

$venvPython = Join-Path ".venv" "Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    Write-Error "No venv interpreter found at .venv\\Scripts\\python.exe. Run ./scripts/bootstrap_venv.ps1 first."
    exit 1
}

& $venvPython -m ruff check .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& $venvPython -m pytest
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if (-not $IncludeRemoteGitHubChecks) {
    exit 0
}

$venvGh = Join-Path ".venv" "Scripts\gh.exe"
if (-not (Test-Path $venvGh)) {
    Write-Error "No venv gh executable found at .venv\\Scripts\\gh.exe. Run ./scripts/bootstrap_venv.ps1 first."
    exit 1
}

$ghToken = & $venvGh auth token
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($ghToken)) {
    Write-Error "Unable to read a GitHub token from the venv gh CLI. Run .\\.venv\\Scripts\\gh.exe auth login first."
    exit 1
}

$env:GH_TOKEN = $ghToken
try {
    & $venvPython "scripts\verify_github_issue_metadata.py" --repo $Repo
    exit $LASTEXITCODE
}
finally {
    Remove-Item Env:GH_TOKEN -ErrorAction SilentlyContinue
}
