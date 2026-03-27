# MindMap 다시서기 공개형 웹앱

이 폴더는 `GitHub Pages` 공개 배포용 정적 웹앱입니다.

## 포함 기능

- 9개 정신건강 척도 선택 및 응답
- 즉시 점수 계산 및 결과 요약
- 브라우저 로컬 저장
- 구글 시트 DB 전송
- 대상자 이름/생년월일 기반 비교 분석 대시보드
- 구글 시트 DB 기준 개인별 검색 및 변화 그래프
- JSON/CSV 백업 및 복원
- A4 결과지 인쇄

## 저장 방식

- 결과는 서버가 아니라 `현재 브라우저 LocalStorage`에 저장됩니다.
- 필요 시 배포된 Apps Script 웹앱 주소를 넣어 `구글 시트 DB`에도 함께 저장할 수 있습니다.
- 다른 기기와 공유하려면 `전체 JSON 저장`으로 백업한 뒤 가져오기 해야 합니다.

## GitHub Pages 배포

- 저장소 루트 `.github/workflows/deploy-pages.yml`이 이 폴더를 배포 대상으로 사용합니다.
- GitHub 저장소 `Settings > Pages`에서 Source를 `GitHub Actions`로 맞추면 됩니다.
