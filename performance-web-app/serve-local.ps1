param(
  [int]$Port = 8123
)

$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

$url = "http://127.0.0.1:$Port/"
Write-Host "[webapp] serving $PSScriptRoot"
Write-Host "[webapp] open $url"
Start-Process $url
py -3.13 -m http.server $Port
