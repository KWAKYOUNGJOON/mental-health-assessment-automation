# Google Sheets AI 상담기록 자동화

## 목적

구글 스프레드시트의 원본 행을 읽어 OpenAI로 분석한 뒤 `AI상담기록` 시트에 상담기록 형태로 저장합니다.

## 동작 방식

- 원본 시트에서 선택한 행 또는 미처리 행을 읽습니다.
- 직접 식별 가능성이 높은 헤더(`이름`, `연락처`, `생년월일` 등)는 프롬프트에서 제외합니다.
- 원본 자유기록 안에 포함된 이름/전화번호/이메일 패턴도 최대한 마스킹한 뒤 AI로 보냅니다.
- 결과는 `AI상담기록` 시트에 원본행 기준으로 저장되며, 다시 실행하면 같은 행은 덮어씁니다.

## 메뉴

- `데이터 자동화 > OpenAI API 키 저장`
- `데이터 자동화 > AI 상담기록 시트 준비`
- `데이터 자동화 > 선택 행 상담기록 생성`
- `데이터 자동화 > 미처리 행 상담기록 생성`

## 설정 시트 키

- `counseling_ai_source_sheet_name`
  - 원본 시트명
- `counseling_ai_output_sheet_name`
  - 결과 시트명
- `counseling_ai_header_rows`
  - 헤더 행 수. 비워두면 자동 감지, 그룹헤더+상세헤더 구조면 `2`
- `counseling_ai_model`
  - 기본값 `gpt-5-mini`
- `counseling_ai_batch_size`
  - 일괄 실행 시 최대 처리 건수
- `counseling_ai_skip_completed`
  - 기존 완료행 건너뛰기 여부

## 권장 사용 순서

1. 대상 스프레드시트의 Apps Script에 현재 `google-apps-script` 폴더 코드를 반영합니다.
2. 스프레드시트를 새로고침합니다.
3. `OpenAI API 키 저장`을 실행합니다.
4. `AI 상담기록 시트 준비`를 실행합니다.
5. `설정` 시트에서 원본 시트명과 헤더 행 수를 확인합니다.
6. 원본 시트에서 행을 선택한 뒤 `선택 행 상담기록 생성`을 실행합니다.
7. 대량 처리 시 `미처리 행 상담기록 생성`을 반복 실행합니다.

## 자동 배포 스크립트

- 로컬에서 바운드 Apps Script 프로젝트를 자동 생성/업데이트하려면 `python-tools/deploy_bound_apps_script.py`를 사용합니다.
- 예시:

```powershell
py -3.13 .\python-tools\deploy_bound_apps_script.py --spreadsheet-id 16LipIyLDQlUdOoppgNSiT7jlmemSToZBHWTydrPaSmw
```

- 현재 계정에서 Apps Script API가 꺼져 있으면 아래 안내와 함께 중단됩니다.
  - `https://script.google.com/home/usersettings` 에서 Apps Script API를 켠 뒤 다시 실행

## 결과 시트 주요 열

- `기록ID`
- `원본시트`
- `원본행번호`
- `처리상태`
- `대상자명`
- `상담일자`
- `상담유형`
- `위험도`
- `주호소`
- `핵심요약`
- `상담기록`
- `후속조치`
- `오류메시지`

## 주의

- 원본 데이터에 자유서술형 상담내용이 거의 없으면 AI 기록도 제한적으로 작성됩니다.
- 외부 AI API로 전송되는 구조이므로 실제 운영 전 개인정보 처리 기준을 반드시 점검하세요.
