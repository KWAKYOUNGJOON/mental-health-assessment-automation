@echo off
chcp 65001 >nul
setlocal

echo ==============================================
echo 정신건강팀 과거 실적 변환
echo ==============================================
echo.
echo 지역별 일일실적 엑셀을 현재 원본데이터 형식으로 변환합니다.
echo 완료되면 같은 폴더에 "_원본데이터변환.xlsx" 파일이 생성됩니다.
echo.

py -3 "%~dp0convert_regional_daily_to_raw_input.py"
if errorlevel 1 (
  echo.
  echo 변환 중 오류가 발생했습니다.
  pause
  exit /b 1
)

echo.
echo 변환이 완료되었습니다.
echo 결과 파일 폴더를 엽니다.
echo.
pause

start "" "%~dp0"
