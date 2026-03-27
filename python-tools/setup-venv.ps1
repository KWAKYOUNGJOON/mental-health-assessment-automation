$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

$venvRoot = Join-Path $env:LOCALAPPDATA "mh-team-automation\venvs"
$venvDir = Join-Path $venvRoot "source-file-py313"
$venvPython = Join-Path $venvDir "Scripts\\python.exe"
$requirements = Join-Path $PSScriptRoot "requirements.txt"

Write-Host "[setup] workspace: $PSScriptRoot"
Write-Host "[setup] venv path: $venvDir"

if (-not (Test-Path -LiteralPath $venvRoot)) {
  New-Item -ItemType Directory -Path $venvRoot | Out-Null
}

if (-not (Test-Path -LiteralPath $venvPython)) {
  Write-Host "[setup] creating virtual environment..."
  py -3.13 -m venv $venvDir
}

Write-Host "[setup] installing requirements..."
& $venvPython -m pip install --disable-pip-version-check -r $requirements

Write-Host "[setup] done."
Write-Host "[setup] activate (PowerShell): $venvDir\\Scripts\\Activate.ps1"
Write-Host "[setup] activate (CMD): $venvDir\\Scripts\\activate.bat"
