# GitHub Pages 공개 배포 안내

## 대상 폴더

- `github-pages-scale-webapp`

## 배포 방식

- GitHub Actions 기반 Pages 배포
- 워크플로: `.github/workflows/deploy-pages.yml`

## 공개 전제

- 이 버전은 서버 없는 정적 웹앱이다.
- 검사 결과는 사용자 브라우저 `LocalStorage`에 저장된다.
- 여러 실무자가 같은 데이터를 함께 쓰려면 이후 별도 백엔드가 필요하다.

## 공개 절차

1. `main` 브랜치에 변경사항을 푸시한다.
2. GitHub 저장소 `Settings > Pages`에서 `Source`를 `GitHub Actions`로 설정한다.
3. `Actions` 탭에서 `Deploy GitHub Pages` 워크플로가 성공했는지 확인한다.
4. 배포 완료 후 발급된 `github.io` 주소를 열어 동작을 확인한다.

## 점검 항목

- 척도 목록 로딩
- 결과 계산
- 로컬 저장
- 비교 분석 차트
- JSON/CSV 내보내기
- 인쇄 미리보기
