# 척도검사 웹앱 Apps Script 웹앱 전환 메모

## 방향

현재 `scale-screening-web-app`은 로컬 Node 서버 기반입니다.

- 화면: `index.html`, `styles.css`, `app.js`
- 로컬 API: `server.js`
- 로컬 저장소: `runtime-data/*.json`
- 구글 시트 연동: 별도 Apps Script Web App

이 구조를 `Apps Script 웹앱 + Google Sheets 저장` 구조로 전환합니다.

## 목표

1. 별도 PC 서버 실행 없이 브라우저 URL로 접속
2. 검사 결과를 Google Sheets에 직접 저장
3. 현장용 입력 화면은 현재 UI 흐름을 최대한 유지
4. 관리자 기능은 2차로 분리 구현

## 권장 단계

### 1차 MVP

- 척도 선택
- 응답자 정보 입력
- 문항 응답
- 결과 계산
- 결과 인쇄
- Google Sheets 저장

### 2차

- 로그인
- 사용자 권한
- 저장 결과 조회
- 대상자 이력
- 위험군 관리

### 3차

- 관리자 계정 관리
- 통계 요약
- 보고서 생성

## 이유

현재 앱은 `/api/*` 라우팅과 로컬 파일 저장을 전제로 합니다.
Apps Script 웹앱은 같은 구조를 그대로 옮기기보다,
`HtmlService + SpreadsheetApp + PropertiesService` 방식으로 재구성하는 편이 안정적입니다.

## 현재 로컬 서버 주요 기능 매핑

| 현재 기능 | 현재 위치 | Apps Script 전환 방향 |
| --- | --- | --- |
| 세션/로그인 | `server.js` | 2차 구현 |
| 설정 저장 | `server.js` + `config.json` | Script Properties 또는 설정 시트 |
| 결과 저장 | `records.json` | `척도검사기록`, `척도문항응답` 시트 |
| 대상자 관리 | `clients.json` | `대상자목록` 시트 |
| 위험군 메모 | `risk-notes.json` | `위험메모` 시트 |
| 척도 마스터 | JSON 파일 | 스크립트 내 상수 또는 마스터 시트 |

## 기술 기준

- 화면 제공: `HtmlService`
- 데이터 저장: `SpreadsheetApp`
- 간단 설정값: `PropertiesService`
- 서버 함수 호출: `google.script.run`
- 웹앱 배포: Apps Script `웹 앱`

## 추천 구현 기준

### 화면

- 로그인 전용 화면보다 `검사 입력 중심` 우선
- 모바일 대응 유지
- 현재 로컬 웹앱의 시각 스타일 최대한 재사용

### 저장

- 현재 사용 중인 Google Sheets 구조 재사용
- 새 웹앱도 같은 스프레드시트에 기록

## 다음 작업

1. Apps Script 웹앱용 기본 폴더 생성
2. `doGet()` + 기본 HTML 화면 생성
3. Google Sheets 저장 함수 연결
4. 현재 척도 데이터 이식
5. 결과 계산 로직 이식
6. 관리자 기능은 별도 단계로 이전
