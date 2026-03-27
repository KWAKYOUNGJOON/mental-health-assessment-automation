# 자동화 코딩 시작 가이드

2026-03-22 기준으로 이 프로젝트의 구조와 실행 준비 상태를 실제로 확인한 결과입니다.

## 한눈에 보기

- `docs`
  - 요구사항, 시트 설계, 구현 체크리스트
- `google-apps-script`
  - Google Sheets 운영 자동화 본체
- `python-tools`
  - 기존 엑셀을 현재 구조로 변환하는 Python 도구
- `performance-web-app`
  - 로컬 브라우저용 업로드/파싱/보고서 웹앱

## 실제 확인된 준비 상태

- Python
  - `py -3.13 --version` 확인됨
  - 공식 작업용 가상환경: `%LOCALAPPDATA%\mh-team-automation\venvs\source-file-py313`
  - `pandas`, `numpy`, `openpyxl`, `openai` import 확인됨
  - `convert_regional_daily_to_raw_input.py --help` 실행 확인됨
- Web App
  - `performance-web-app\serve-local.ps1` 준비됨
  - 브라우저 직열기보다 로컬 서버 실행 방식이 안전함
- Apps Script
  - 소스는 준비되어 있음
  - 현재 PC에는 `node`, `npm`, `clasp`가 없음
  - 따라서 로컬 `clasp push` 흐름은 아직 바로 실행할 수 없음
- 기타
  - 현재 PC에서는 `git` 명령도 잡히지 않음

## 바로 시작할 때 추천 경로

### 1. 엑셀 변환 자동화부터 볼 때

- 시작 파일: `python-tools\convert_regional_daily_to_raw_input.py`
- 관련 파일:
  - `python-tools\process_excel.py`
  - `python-tools\requirements.txt`
- 권장 명령:

```powershell
cd ".\python-tools"
.\activate-venv.ps1
python .\convert_regional_daily_to_raw_input.py --help
```

### 2. 업로드/보고서 웹앱부터 볼 때

- 시작 파일: `performance-web-app\app.js`
- 관련 파일:
  - `performance-web-app\index.html`
  - `performance-web-app\parser-config.json`
  - `performance-web-app\styles.css`
- 권장 명령:

```powershell
cd ".\performance-web-app"
.\serve-local.ps1
```

### 3. 구글시트 자동화부터 볼 때

- 시작 파일:
  - `google-apps-script\EntryTriggers.gs`
  - `google-apps-script\Config.gs`
  - `google-apps-script\Dashboard.gs`
- 참고 문서:
  - `docs\apps-script-detailed-design.md`
  - `docs\implementation-checklist-google-sheets-automation.md`
- 현재 제약:
  - 로컬 `npm install`, `npx clasp login`, `npx clasp push`는 Node 설치 전까지 보류
  - 즉시 작업은 소스 수정과 구조 검토 중심으로 가능

## 추천 읽기 순서

1. `README.md`
2. `START-HERE.md`
3. `docs\pdr-regional-daily-performance-google-sheets-automation.md`
4. 필요한 구현 폴더의 `README.md`

## 문서 기준 우선 작업 후보

- Google Sheets MVP는 문서상 `방식 A(집계값 입력)`가 기본안으로 정리되어 있음
- 체크리스트 기준 핵심 자동화 포인트는 아래 순서가 자연스러움
  - `일일활동DB` 입력 구조 확정
  - `onEdit` 입력자/입력일시 자동 기록
  - 날짜 유효성 보정 및 중복 경고
  - 월별뷰/보고 시트 재계산 보조

## 작업 메모

- `python-tools` 안에 `.venv`, `.venv313` 폴더가 있어도 공식 안내는 `%LOCALAPPDATA%` 가상환경 기준입니다.
- 공유 드라이브 경로에서는 `pip install`이 느려질 수 있어, 루트 문서와 스크립트도 로컬 가상환경 방식을 전제로 하고 있습니다.
- 웹앱은 `parser-config.json`을 읽기 때문에 `index.html` 직접 열기보다 `serve-local.ps1` 실행이 더 안정적입니다.
