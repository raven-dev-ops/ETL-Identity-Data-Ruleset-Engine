param(
    [string]$Bundle = "",
    [string]$BundleRoot = "",
    [string]$InstallRoot = "",
    [string]$Output = "",
    [string]$TrustedPublicKey = "",
    [string]$Python = "python",
    [int]$DemoPort = 8000,
    [int]$ServicePort = 8010,
    [double]$MinFreeGiB = 2.0
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$tool = Join-Path $root "tools\check_pilot_readiness.py"
if (-not (Test-Path $tool)) {
    throw "Expected bundle readiness tool was not found at $tool."
}

$arguments = @($tool)
if (-not [string]::IsNullOrWhiteSpace($Bundle)) { $arguments += @("--bundle", $Bundle) }
if (-not [string]::IsNullOrWhiteSpace($BundleRoot)) { $arguments += @("--bundle-root", $BundleRoot) }
if (-not [string]::IsNullOrWhiteSpace($InstallRoot)) { $arguments += @("--install-root", $InstallRoot) }
if (-not [string]::IsNullOrWhiteSpace($Output)) { $arguments += @("--output", $Output) }
if (-not [string]::IsNullOrWhiteSpace($TrustedPublicKey)) { $arguments += @("--trusted-public-key", $TrustedPublicKey) }
if ([string]::IsNullOrWhiteSpace($Bundle) -and [string]::IsNullOrWhiteSpace($BundleRoot)) {
    $arguments += @("--bundle-root", $root)
}
$arguments += @("--demo-port", [string]$DemoPort)
$arguments += @("--service-port", [string]$ServicePort)
$arguments += @("--min-free-gib", [string]$MinFreeGiB)

& $Python @arguments
