$ErrorActionPreference = "Stop"
$venvDir = Join-Path $env:LOCALAPPDATA "mh-team-automation\venvs\source-file-py313"
$activatePath = Join-Path $venvDir "Scripts\\Activate.ps1"

if (-not (Test-Path -LiteralPath $activatePath)) {
  Write-Error "가상환경을 찾지 못했습니다. 먼저 setup-venv.ps1를 실행하세요."
}

. $activatePath
