@echo off
setlocal
set "VENV_DIR=%LOCALAPPDATA%\mh-team-automation\venvs\source-file-py313"

if not exist "%VENV_DIR%\Scripts\activate.bat" (
  echo [activate] virtual environment not found. Run setup-venv.ps1 first.
  exit /b 1
)

call "%VENV_DIR%\Scripts\activate.bat"
