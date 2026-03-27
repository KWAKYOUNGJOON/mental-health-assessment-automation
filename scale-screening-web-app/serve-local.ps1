param(
  [int]$Port = 8134
)

$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

$url = "http://127.0.0.1:$Port/"
$healthUrl = "http://127.0.0.1:$Port/api/health"
Write-Host "[scale-webapp] serving $PSScriptRoot"
Write-Host "[scale-webapp] open $url"

try {
  Invoke-WebRequest -UseBasicParsing -Uri $healthUrl -TimeoutSec 2 | Out-Null
  Write-Host "[scale-webapp] server already running"
  Start-Process $url
  return
} catch {
  Write-Host "[scale-webapp] starting local server..."
}

Start-Job -ArgumentList $healthUrl, $url -ScriptBlock {
  param($InnerHealthUrl, $InnerUrl)

  for ($i = 0; $i -lt 40; $i++) {
    try {
      Invoke-WebRequest -UseBasicParsing -Uri $InnerHealthUrl -TimeoutSec 2 | Out-Null
      Start-Process $InnerUrl
      break
    } catch {
      Start-Sleep -Milliseconds 500
    }
  }
} | Out-Null

node .\server.js $Port
