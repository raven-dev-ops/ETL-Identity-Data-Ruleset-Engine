param(
    [string]$Bundle = "",
    [string]$BundleRoot = "",
    [string]$InstallRoot = "",
    [string]$Python = "python",
    [int]$PostgresPort = 0,
    [string]$PostgresContainerName = "",
    [string]$PostgresDb = "identity_state",
    [string]$PostgresUser = "etl_identity",
    [string]$PostgresPassword = "pilot-password",
    [string]$DemoHost = "127.0.0.1",
    [int]$DemoPort = 8000,
    [int]$ServicePort = 8010,
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"
$scriptRoot = Split-Path -Parent $PSCommandPath
$pythonScript = Join-Path $scriptRoot "bootstrap_windows_customer_pilot.py"

$arguments = @($pythonScript)
if (-not [string]::IsNullOrWhiteSpace($Bundle)) { $arguments += @("--bundle", $Bundle) }
if (-not [string]::IsNullOrWhiteSpace($BundleRoot)) { $arguments += @("--bundle-root", $BundleRoot) }
if (-not [string]::IsNullOrWhiteSpace($InstallRoot)) { $arguments += @("--install-root", $InstallRoot) }
if ($PostgresPort -gt 0) { $arguments += @("--postgres-port", [string]$PostgresPort) }
if (-not [string]::IsNullOrWhiteSpace($PostgresContainerName)) { $arguments += @("--postgres-container-name", $PostgresContainerName) }
$arguments += @("--python", $Python)
$arguments += @("--postgres-db", $PostgresDb)
$arguments += @("--postgres-user", $PostgresUser)
$arguments += @("--postgres-password", $PostgresPassword)
$arguments += @("--demo-host", $DemoHost)
$arguments += @("--demo-port", [string]$DemoPort)
$arguments += @("--service-port", [string]$ServicePort)
if ($PrepareOnly.IsPresent) { $arguments += "--prepare-only" }

& $Python @arguments
