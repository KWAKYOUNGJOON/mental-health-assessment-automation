# Python Utilities

`python-tools` 폴더에는 실적 엑셀 변환 스크립트와 AI 데모 스크립트가 함께 있습니다.

## 파일 설명

- `convert_regional_daily_to_raw_input.py`
  - 과거 지역별 일일실적 엑셀을 현재 구글시트 `원본데이터` 형식으로 변환
  - 출력:
    - `*_원본데이터변환.xlsx`
    - `*_구글시트붙여넣기용.xlsx`
    - `*_구글시트붙여넣기용.tsv`
- `process_excel.py`
  - `기본DB` 시트를 읽어 `기본정보`, `병원기록`, `시설기록`, `서비스기록` 시트로 분리
- `openai_analysis_demo.py`
  - `OPENAI_API_KEY`를 사용한 상담 분류 예제
- `ai_analysis_demo.py`
  - `GEMINI_API_KEY`를 사용한 상담 분류 예제

## 빠른 시작

1. `setup-venv.ps1` 실행
   - 또는 `setup-venv.cmd` 실행
2. 가상환경 활성화
   - PowerShell: `.\activate-venv.ps1`
   - CMD: `activate-venv.cmd`
3. 필요한 스크립트 실행

## 직접 명령으로 준비하기

```powershell
cd "H:\공유 드라이브\정신건강팀공유드라이브\정신건강팀자동화프로젝트\project-01-performance-automation\python-tools"
.\setup-venv.ps1
```

가상환경은 공유 드라이브 안이 아니라 아래 로컬 경로에 생성됩니다.

```text
%LOCALAPPDATA%\mh-team-automation\venvs\source-file-py313
```

이 방식은 `H:\공유 드라이브` 경로에서 `pip install`이 비정상적으로 오래 멈추는 문제를 피하기 위한 설정입니다.

## 예시 실행

```powershell
& "$env:LOCALAPPDATA\mh-team-automation\venvs\source-file-py313\Scripts\python.exe" .\convert_regional_daily_to_raw_input.py
& "$env:LOCALAPPDATA\mh-team-automation\venvs\source-file-py313\Scripts\python.exe" .\process_excel.py
```

입력 파일을 직접 지정하려면:

```powershell
& "$env:LOCALAPPDATA\mh-team-automation\venvs\source-file-py313\Scripts\python.exe" .\convert_regional_daily_to_raw_input.py --input ".\2026 지역별 일일실적 정신건강팀 실적.xlsx"
```

## 환경 변수

AI 데모 스크립트는 아래 환경 변수를 사용합니다.

- `OPENAI_API_KEY`
- `GEMINI_API_KEY`

`.env.example`를 참고해서 값을 정리한 뒤, PowerShell 세션에서 다음처럼 설정할 수 있습니다.

```powershell
$env:OPENAI_API_KEY="your_key"
$env:GEMINI_API_KEY="your_key"
```

## 메모

- 현재 PC에는 전역 `pandas`, `openai`, `google-generativeai`가 설치되어 있지 않아 로컬 가상환경 기준 작업을 권장합니다.
- 현재 PC에서는 `py -3.13`이 안정적으로 잡히므로 `.ps1` 경로를 우선 권장합니다.
- 결과 엑셀과 캐시 파일이 같은 폴더에 생성되므로 원본 파일은 별도로 보관하는 편이 안전합니다.
