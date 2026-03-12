Param(
    [string]$BaseDir = ".",
    [ValidateSet("small", "medium", "large")]
    [string]$Profile = "small",
    [int]$Seed = 42
)

$venvPython = Join-Path ".venv" "Scripts\python.exe"
if (Test-Path $venvPython) {
    & $venvPython -m etl_identity_engine.cli run-all --base-dir $BaseDir --profile $Profile --seed $Seed
    exit $LASTEXITCODE
}

Write-Error "No venv interpreter found at .venv\\Scripts\\python.exe. Run ./scripts/bootstrap_venv.ps1 first."
exit 1
