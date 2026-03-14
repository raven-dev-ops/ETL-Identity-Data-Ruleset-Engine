Param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CliArgs
)

$venvPython = Join-Path ".venv" "Scripts\python.exe"
if (Test-Path $venvPython) {
    & $venvPython "scripts\run_pipeline.py" @CliArgs
    exit $LASTEXITCODE
}

Write-Error "No venv interpreter found at .venv\\Scripts\\python.exe. Run ./scripts/bootstrap_venv.ps1 first."
exit 1
