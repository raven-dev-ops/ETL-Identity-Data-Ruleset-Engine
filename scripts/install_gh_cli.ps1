Param(
    [string]$VenvPath = ".venv",
    [string]$Version = "2.88.0"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$venvScripts = Join-Path $VenvPath "Scripts"
$venvPython = Join-Path $venvScripts "python.exe"
if (-not (Test-Path $venvPython)) {
    throw "Venv python not found at $venvPython. Run bootstrap_venv.ps1 first."
}

$toolsRoot = Join-Path $VenvPath "tools"
$downloadsDir = Join-Path $toolsRoot "downloads"
$ghExtractDir = Join-Path $toolsRoot "gh"
New-Item -ItemType Directory -Force -Path $downloadsDir | Out-Null
New-Item -ItemType Directory -Force -Path $ghExtractDir | Out-Null

$zipName = "gh_{0}_windows_amd64.zip" -f $Version
$zipPath = Join-Path $downloadsDir $zipName
$url = "https://github.com/cli/cli/releases/download/v{0}/{1}" -f $Version, $zipName

Write-Host "Downloading GitHub CLI v$Version..."
curl.exe -L $url -o $zipPath

Write-Host "Extracting GitHub CLI..."
Expand-Archive -Path $zipPath -DestinationPath $ghExtractDir -Force

$ghExe = Get-ChildItem -Path $ghExtractDir -Filter gh.exe -Recurse | Select-Object -First 1 -ExpandProperty FullName
if (-not $ghExe) {
    throw "gh.exe not found after extraction."
}

$targetExe = Join-Path $venvScripts "gh.exe"
Copy-Item -Path $ghExe -Destination $targetExe -Force

Write-Host "Installed gh to: $targetExe"
& $targetExe --version

