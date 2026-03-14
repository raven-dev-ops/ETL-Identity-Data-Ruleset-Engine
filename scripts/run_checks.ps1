Param(
    [switch]$IncludeRemoteGitHubChecks,
    [string]$Repo = "raven-dev-ops/ETL-Identity-Data-Ruleset-Engine"
)

$venvPython = Join-Path ".venv" "Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    Write-Error "No venv interpreter found at .venv\\Scripts\\python.exe. Run ./scripts/bootstrap_venv.ps1 first."
    exit 1
}

$arguments = @(
    "scripts\run_checks.py",
    "--repo", $Repo
)

if ($IncludeRemoteGitHubChecks) {
    $arguments += "--include-remote-github-checks"
}

& $venvPython @arguments
exit $LASTEXITCODE
