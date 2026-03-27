$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

Write-Host "== 프로젝트 1: 정신건강팀 실적 자동화 =="
Write-Host ""
Write-Host "[구성]"
Write-Host "- docs"
Write-Host "- google-apps-script"
Write-Host "- python-tools"
Write-Host "- performance-web-app"
Write-Host ""

Write-Host "[Python]"
try {
  $pyVersion = & py -3.13 --version
  Write-Host "- py: $pyVersion"
} catch {
  Write-Host "- py -3.13: 미확인"
}

$venvPython = Join-Path $env:LOCALAPPDATA "mh-team-automation\venvs\source-file-py313\Scripts\python.exe"
if (Test-Path -LiteralPath $venvPython) {
  Write-Host "- venv: 준비됨"
  & $venvPython -c "import pandas, numpy, openpyxl; print('- python libs: ready')"
} else {
  Write-Host "- venv: 없음 -> .\\python-tools\\setup-venv.ps1 실행 필요"
}

Write-Host ""
Write-Host "[Web App]"
$webScript = Join-Path $PSScriptRoot "performance-web-app\serve-local.ps1"
if (Test-Path -LiteralPath $webScript) {
  Write-Host "- web app launcher: 준비됨"
} else {
  Write-Host "- web app launcher: 없음"
}

Write-Host ""
Write-Host "[Apps Script]"
if (Get-Command node -ErrorAction SilentlyContinue) {
  Write-Host "- node: 설치됨"
} else {
  Write-Host "- node: 미설치"
}

if (Get-Command clasp -ErrorAction SilentlyContinue) {
  Write-Host "- clasp: 설치됨"
} else {
  Write-Host "- clasp: 미설치"
}

Write-Host ""
Write-Host "[다음 명령]"
Write-Host "1) Python"
Write-Host '   cd ".\python-tools"; .\activate-venv.ps1'
Write-Host "2) Web App"
Write-Host '   cd ".\performance-web-app"; .\serve-local.ps1'
Write-Host "3) Apps Script"
Write-Host '   cd ".\google-apps-script"; npm install; npx clasp login'
