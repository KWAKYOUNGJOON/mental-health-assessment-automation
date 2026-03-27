@echo off
setlocal
cd /d "%~dp0"
set "PORT=8123"
if not "%~1"=="" set "PORT=%~1"
powershell -ExecutionPolicy Bypass -File "%~dp0serve-local.ps1" -Port %PORT%
