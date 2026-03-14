Param(
    [Parameter(Mandatory = $true)]
    [string]$Repo,

    [string]$BacklogPath = "planning/active-github-issues-backlog.md",

    [switch]$DryRun,

    [switch]$IncludeClosed
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-PythonExecutable {
    $candidates = @(
        (Join-Path ".venv" "Scripts\python.exe"),
        (Join-Path ".venv" "bin\python"),
        "python"
    )

    foreach ($candidate in $candidates) {
        if ($candidate -eq "python") {
            $command = Get-Command "python" -ErrorAction SilentlyContinue
            if ($command) {
                return $command.Source
            }
            continue
        }

        if (Test-Path $candidate) {
            return (Resolve-Path $candidate).Path
        }
    }

    throw "Python executable not found. Run the bootstrap script or install Python on PATH."
}

$pythonExe = Resolve-PythonExecutable
$arguments = @(
    "scripts/create_github_backlog.py",
    "--repo", $Repo,
    "--backlog-path", $BacklogPath
)

if ($DryRun) {
    $arguments += "--dry-run"
}

if ($IncludeClosed) {
    $arguments += "--include-closed"
}

& $pythonExe @arguments
if ($LASTEXITCODE -ne 0) {
    throw "create_github_backlog.py failed with exit code $LASTEXITCODE"
}
