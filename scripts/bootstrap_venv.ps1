Param(
    [string]$VenvPath = ".venv",
    [string]$PythonCommand = "",
    [bool]$InstallGh = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-PythonExecutable {
    param([string]$Requested)

    if ($Requested) {
        $candidate = Get-Command $Requested -ErrorAction SilentlyContinue
        if ($candidate) {
            if ($candidate.Source -like "*WindowsApps*") {
                throw "Requested command resolves to Windows Store alias: $Requested. Install real Python and retry."
            }
            return $candidate.Source
        }
        throw "Requested Python command not found: $Requested"
    }

    $options = @("python", "python3", "py")
    foreach ($name in $options) {
        $candidate = Get-Command $name -ErrorAction SilentlyContinue
        if (-not $candidate) {
            continue
        }

        $source = $candidate.Source
        if ($source -like "*WindowsApps*") {
            continue
        }
        return $source
    }

    $knownPaths = @(
        "$env:LocalAppData\Programs\Python\Python312\python.exe",
        "$env:LocalAppData\Programs\Python\Python311\python.exe",
        "$env:ProgramFiles\Python312\python.exe",
        "$env:ProgramFiles\Python311\python.exe",
        "C:\Python312\python.exe",
        "C:\Python311\python.exe"
    )
    foreach ($path in $knownPaths) {
        if (Test-Path $path) {
            return $path
        }
    }

    throw "No usable Python interpreter found. Install Python 3.11+ and re-run."
}

$pythonExe = Resolve-PythonExecutable -Requested $PythonCommand
Write-Host "Using Python: $pythonExe"

& $pythonExe -m venv $VenvPath

$venvPython = Join-Path $VenvPath "Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    throw "Failed to create venv at $VenvPath"
}

& $venvPython -m pip install --upgrade pip setuptools wheel
& $venvPython -m pip install -e ".[dev]"

if ($InstallGh) {
    $ghInstaller = Join-Path $PSScriptRoot "install_gh_cli.ps1"
    if (Test-Path $ghInstaller) {
        & $ghInstaller -VenvPath $VenvPath
    }
}

Write-Host ""
Write-Host "Bootstrap complete."
Write-Host "Activate with:"
Write-Host "  .\\$VenvPath\\Scripts\\Activate.ps1"
Write-Host "Then run:"
Write-Host "  ruff check ."
Write-Host "  pytest"
Write-Host "  python -m etl_identity_engine.cli run-all"
Write-Host "  gh --version"
Write-Host "  ./scripts/run_checks.ps1"
Write-Host ""
Write-Host "Optional deployed-state check after pushing:"
Write-Host "  ./scripts/run_checks.ps1 -IncludeRemoteGitHubChecks"
