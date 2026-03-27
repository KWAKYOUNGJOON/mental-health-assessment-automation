# Apps Script 상세 설계서: 지역별 일일실적 구글시트 자동화

관련 문서:

- [PDR](./pdr-regional-daily-performance-google-sheets-automation.md)
- [구현 체크리스트](./implementation-checklist-google-sheets-automation.md)
- [구글시트 탭 설계서](./google-sheets-tab-design.md)

## 1. 목적

Apps Script는 수식으로 해결 가능한 집계 로직을 대체하지 않는다. 대신 아래 역할에 집중한다.

- 입력 보조: 타임스탬프, 입력자, 날짜 보정
- 운영 보조: 관리자 메뉴, 검증 실행, 월별뷰 갱신
- 선택 자동화: 월말 PDF 발송, 로그 기록

즉, **집계는 수식**, **운영 편의는 스크립트** 원칙을 따른다.

## 2. 프로젝트 구조 권장안

Apps Script 파일은 기능별로 분리한다.

| 파일명 | 역할 |
| --- | --- |
| `Config.gs` | 시트명, 열 번호, 상수, 메시지 정의 |
| `EntryTriggers.gs` | `onOpen`, `onEdit` 트리거 |
| `Validation.gs` | 날짜/숫자/중복 검증 로직 |
| `MonthlyView.gs` | 월별뷰 갱신 및 재계산 |
| `Reports.gs` | PDF 생성 및 메일 발송 |
| `AdminMenu.gs` | 사용자 메뉴와 관리 실행 함수 |
| `Log.gs` | 운영로그 기록 함수 |

## 3. 상수 설계

### 3.1 시트명 상수

```javascript
const SHEETS = {
  CONFIG: "기준",
  GUIDE: "도움말",
  DAILY_DB: "일일활동DB",
  DAILY_NORMALIZED: "일일활동정규화",
  DAILY_VIEW: "일일실적_월별뷰",
  HOSPITAL: "입원현황",
  FACILITY: "시설입소현황",
  REFERRAL: "연계대장",
  POST_ACTION: "조치후 사례관리",
  MONTHLY_REPORT: "법인월보고용",
  QUARTERLY_REPORT: "분기보고용",
  OPS_LOG: "운영로그"
};
```

### 3.2 `일일활동DB` 열 인덱스 상수

```javascript
const DAILY_DB_COL = {
  DATE: 1,
  WORKER: 2,
  MENTAL: 3,
  ALCOHOL: 4,
  ETC_COUNSEL: 5,
  VOLUNTARY_ADMISSION: 6,
  EMERGENCY_ADMISSION: 7,
  DIAGNOSIS_PROTECT: 8,
  FACILITY_HOMELESS: 9,
  FACILITY_WELFARE: 10,
  SUPPORT_HOUSING: 11,
  HOUSING_SUPPORT: 12,
  SUPPLIES: 13,
  ETC_ACTION: 14,
  OUTPATIENT: 15,
  HOSPITAL_VISIT: 16,
  HOME_VISIT: 17,
  MEDICATION: 18,
  ETC_CASE: 19,
  EDITOR: 20,
  EDITED_AT: 21
};
```

## 4. 트리거 설계

### 4.1 `onOpen(e)`

목적:

- 관리자/사용자 공통 메뉴 표시
- 빠른 검증 및 재계산 진입점 제공

메뉴 예시:

- `자동화 도구 > 입력 검증 실행`
- `자동화 도구 > 월별뷰 갱신`
- `자동화 도구 > 운영로그 열기`
- `자동화 도구 > 당월 보고서 PDF 생성`

### 4.2 `onEdit(e)`

적용 범위:

- `일일활동DB`

동작 순서:

1. 수정 시트가 `일일활동DB`인지 확인
2. 헤더 행인지 확인하고 헤더면 종료
3. 날짜/담당자/건수 영역(A:S) 수정인지 확인
4. 날짜 보정 로직 실행
5. 값 유효성 점검
6. T/U 열에 입력자/입력일시 기록
7. 중복 여부 점검
8. 운영로그 기록

비적용 범위:

- 집계 시트
- `T:U` 자동 기록 열 직접 수정

### 4.3 시간 기반 트리거

| 트리거 | 주기 | 목적 | 기본 여부 |
| --- | --- | --- | --- |
| `sendMonthlyReport` | 매월 말일 오후 | 법인월보고용 PDF 발송 | 선택 |
| `runMonthlyValidation` | 매일 오전 | 데이터 검증 로그 생성 | 선택 |

## 5. 핵심 함수 설계

### 5.1 `handleDailyDbEdit_(e)`

책임:

- `onEdit` 이벤트의 실제 처리 담당
- 입력 행 단위 검증 및 후처리

입력:

- 이벤트 객체 `e`

출력:

- 없음

실패 처리:

- 예외 발생 시 `운영로그` 기록
- 치명적 오류가 아니면 사용자 편집은 되돌리지 않고 관리자 확인 대상으로 남김

### 5.2 `normalizeDateValue_(value, timezone)`

책임:

- 텍스트 날짜를 시트 날짜값으로 변환 시도
- 허용 형식 외에는 실패로 처리

권장 허용 패턴:

- `2026-03-16`
- `2026/03/16`
- `3/16` 입력 시 현재 기준 연도 보정 여부는 정책 결정 필요

권장안:

- 자동 보정은 `YYYY-MM-DD`, `YYYY/MM/DD`까지만 수행
- `3/16`은 경고만 띄우고 사용자가 다시 입력하게 한다

이유:

- 연도 추정 자동 보정은 과거 데이터 입력 시 오류 위험이 높다.

### 5.3 `stampEditor_(sheet, row)`

책임:

- 입력자와 입력일시 기록

기록 규칙:

- 입력자: 가능하면 `Session.getActiveUser().getEmail()`
- 이메일을 가져올 수 없는 환경이면 `"unknown_user"`
- 입력일시: `new Date()`

주의:

- 개인 Gmail 환경에서는 사용자 이메일 확보가 제한될 수 있다.
- 이 경우 입력자 컬럼 정책을 별도로 정해야 한다.

### 5.4 `detectDuplicateDailyKey_(sheet, row)`

중복 키 정의:

- `날짜 + 담당자`

처리 원칙:

- 저장 차단은 하지 않음
- 노트, 배경색 또는 토스트 메시지로 경고
- 운영로그에는 `WARN` 수준으로 남김

이유:

- 중복 입력이 항상 오류는 아니고, 같은 날 추가 입력이 필요한 실제 상황이 있을 수 있다.
- 집계는 `SUMIFS`가 자동 합산하므로 차단보다는 알림이 적합하다.

### 5.5 `runValidationReport()`

검증 항목 예시:

- `일일활동DB`의 날짜 누락 행 수
- 담당자 누락 행 수
- 음수 값 존재 여부
- 중복 키 개수
- `입원실적`과 `입원현황` 차이
- `연계실적`과 `연계대장` 차이

출력 방식:

- `운영로그`에 결과 적재
- 필요 시 `SpreadsheetApp.getActive().toast()`로 요약 알림

### 5.6 `refreshMonthlyView()`

책임:

- `일일실적_월별뷰`의 연도/월 파라미터에 맞춰 날짜 블록과 헤더 갱신
- 28/29/30/31일 이외 영역을 비우거나 숨김 표시

구현 원칙:

- 수식은 최대한 시트에 유지
- 스크립트는 날짜 라벨, 주차 라벨, 보조 표시만 갱신

### 5.7 `sendMonthlyReport()`

책임:

- `법인월보고용` 시트를 PDF로 내보내고 메일 발송

전제:

- 관리자 메일 주소가 `기준` 시트에 설정되어 있음
- 사용자 계정이 메일 발송 권한을 가짐

실패 처리:

- 메일 발송 실패 시 `운영로그`에 `ERROR` 기록
- 사용자에게 토스트 표시

## 6. 의사코드

### 6.1 `onEdit` 처리 흐름

```javascript
function onEdit(e) {
  try {
    if (!isDailyDbEdit_(e)) return;
    handleDailyDbEdit_(e);
  } catch (error) {
    writeLog_("ERROR", "onEdit failed", { message: error.message });
  }
}
```

### 6.2 편집 후처리 흐름

```javascript
function handleDailyDbEdit_(e) {
  const sheet = e.range.getSheet();
  const row = e.range.getRow();

  if (row === 1) return;

  normalizeEditedDateIfNeeded_(sheet, row);
  validateEditedRow_(sheet, row);
  stampEditor_(sheet, row);
  detectDuplicateDailyKey_(sheet, row);
  writeLog_("INFO", "daily db updated", { row });
}
```

## 7. 로그 설계

### 7.1 `운영로그` 시트 열 구조

| 열 | 필드명 | 설명 |
| --- | --- | --- |
| A | 기록시각 | 로그 기록 시각 |
| B | 수준 | INFO / WARN / ERROR |
| C | 이벤트 | 예: `daily_db_edit`, `validation_run` |
| D | 사용자 | 가능하면 이메일 |
| E | 시트명 | 대상 시트 |
| F | 행번호 | 대상 행 |
| G | 메시지 | 요약 메시지 |
| H | 상세JSON | 디버깅용 상세 |

### 7.2 로그 정책

- INFO: 일반 편집, 수동 검증 실행
- WARN: 중복 입력, 날짜 보정 실패, 집계 불일치
- ERROR: 스크립트 예외, 메일 발송 실패

## 8. 권한 및 보안 고려사항

- `Session.getActiveUser().getEmail()`은 계정 정책에 따라 빈 값일 수 있다.
- 보호 시트가 있을 경우 스크립트 실행 계정이 수정 권한을 가져야 한다.
- 메일 발송 기능은 관리자 계정에서만 동작하도록 제한하는 것이 안전하다.
- 민감정보가 포함된 케이스 DB는 로그에 직접 남기지 않는다.

## 9. 예외 처리 정책

| 상황 | 처리 원칙 |
| --- | --- |
| 날짜 파싱 실패 | 사용자에게 경고, 로그 기록 |
| 입력자 이메일 미확인 | `unknown_user` 기록 |
| 중복 입력 감지 | 차단하지 않고 경고 |
| 집계 검증 불일치 | `WARN` 로그 + 관리자 재검토 |
| 메일 발송 실패 | `ERROR` 로그 + 수동 발송 절차 안내 |

## 10. 배포 순서

1. 시트 구조와 수식을 먼저 확정한다.
2. Apps Script 기본 상수와 메뉴를 배포한다.
3. `onEdit` 타임스탬프와 검증 기능을 배포한다.
4. 검증 메뉴와 로그 시트를 붙인다.
5. 마지막으로 메일 발송 같은 선택 자동화를 붙인다.

이 순서를 권장하는 이유는, 수식 기반 집계가 먼저 안정되어야 스크립트 문제와 수식 문제를 분리해서 볼 수 있기 때문이다.

## 11. MVP 범위와 후속 범위

### MVP 포함

- `onOpen`
- `onEdit`
- 날짜 보정
- 입력자/입력일시 기록
- 중복 경고
- 수동 검증 메뉴

### 후속 확장

- 월말 PDF 자동메일
- 월별뷰 자동 구조 재배치
- 관리자 대시보드
- 모바일 입력 폼 연동

