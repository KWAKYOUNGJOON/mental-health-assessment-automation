# 프로젝트 1. 정신건강팀 실적 자동화

## 코딩 시작 전

- 실제 실행 기준 시작 가이드: [`START-HERE.md`](./START-HERE.md)

## 프로젝트 범위

이 프로젝트는 아래 5개 폴더가 함께 동작하는 구조입니다.

- `docs`
- `google-apps-script`
- `github-pages-scale-webapp`
- `python-tools`
- `performance-web-app`

즉, 개별 폴더 5개가 아니라 하나의 실적 자동화 프로젝트를 구성하는 부품들입니다.

## 폴더별 역할

### `docs`

- 요구사항
- 시트 설계
- 구현 체크리스트
- 상세 설계 문서

### `google-apps-script`

- Google Sheets 기반 운영 자동화 본체
- `원본데이터`를 읽어 정규화 시트와 대시보드를 생성
- Apps Script 편집기 또는 `clasp` 연동 방식으로 배포

### `github-pages-scale-webapp`

- GitHub Pages 공개 배포용 정적 척도검사 웹앱
- 서버 없이 브라우저 로컬 저장 기반으로 동작
- 비교 분석 대시보드와 JSON/CSV 백업 지원

### `python-tools`

- 과거 엑셀을 현재 구조에 맞게 변환하는 Python 도구
- AI 분류 데모 스크립트 포함
- 로컬 가상환경 위치:
  - `%LOCALAPPDATA%\mh-team-automation\venvs\source-file-py313`

### `performance-web-app`

- 실적 업로드/조회/보고서 생성용 로컬 웹앱
- 실행 스크립트:
  - `serve-local.ps1`

## 현재 준비 상태

### Python

- `py -3.13` 확인 완료
- 로컬 가상환경 생성 완료
- `requirements.txt` 설치 완료
- `convert_regional_daily_to_raw_input.py --help` 실행 완료
- `process_excel.py` 실행 완료

### 웹앱

- `serve-local.ps1` 준비 완료
- `parser-config.json` 로딩을 고려해 로컬 서버 방식으로 실행 가능

### Apps Script

- `package.json` 추가 완료
- `node`, `clasp`는 아직 설치되지 않음

## 시작 순서

### Python 작업부터 시작할 때

```powershell
cd "H:\공유 드라이브\정신건강팀공유드라이브\정신건강팀자동화프로젝트\project-01-performance-automation\python-tools"
.\activate-venv.ps1
python .\convert_regional_daily_to_raw_input.py --help
```

### 웹앱부터 시작할 때

```powershell
cd "H:\공유 드라이브\정신건강팀공유드라이브\정신건강팀자동화프로젝트\project-01-performance-automation\performance-web-app"
.\serve-local.ps1
```

### 공개 웹앱 작업을 시작할 때

```powershell
cd "H:\공유 드라이브\정신건강팀공유드라이브\정신건강팀자동화프로젝트\project-01-performance-automation\github-pages-scale-webapp"
```

### Apps Script 작업을 시작할 때

Node.js 설치 후:

```powershell
cd "H:\공유 드라이브\정신건강팀공유드라이브\정신건강팀자동화프로젝트\project-01-performance-automation\google-apps-script"
npm install
npx clasp login
npx clasp push
```
