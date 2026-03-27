const APP_CONFIG = {
  timezone: "Asia/Seoul",
  menuTitle: "데이터 자동화",
  sourceHeaderRows: 2,
  rawSheetFallbackNames: ["raw_input", "시트1", "Sheet1"],
  sheetNames: {
    rawInput: "원본데이터",
    base: "기본정보",
    hospital: "병원기록",
    facility: "시설기록",
    service: "서비스기록",
    publicService: "공적서비스",
    counselingAi: "AI상담기록",
    monthly: "월간실적",
    error: "오류로그",
    dashboard: "요약대시보드",
    settings: "설정"
  },
  headers: {
    base: [
      "고유번호",
      "날짜",
      "담당자",
      "이름",
      "생년월일_원본",
      "생년월일_표준화",
      "성별",
      "상담유형",
      "유효성"
    ],
    hospital: [
      "고유번호",
      "날짜",
      "담당자",
      "이름",
      "생년월일",
      "성별",
      "입원유형",
      "병원명",
      "입원여부",
      "퇴원후연계"
    ],
    facility: [
      "고유번호",
      "날짜",
      "담당자",
      "이름",
      "생년월일",
      "성별",
      "입소유형",
      "시설명",
      "입퇴소여부",
      "퇴소후조치"
    ],
    service: [
      "서비스번호",
      "고유번호",
      "날짜",
      "담당자",
      "이름",
      "생년월일",
      "성별",
      "서비스유형",
      "서비스상세",
      "관련기관명"
    ],
    publicService: [
      "서비스번호",
      "고유번호",
      "날짜",
      "담당자",
      "이름",
      "생년월일",
      "성별",
      "지원항목",
      "신청기관",
      "지원시기"
    ],
    counselingAi: [
      "상담일자",
      "대상자명",
      "상담유형",
      "AI입력텍스트",
      "처리상태",
      "상담기록",
      "후속조치",
      "주호소",
      "핵심요약",
      "키워드",
      "사용모델",
      "오류메시지"
      ,"기록ID",
      "원본시트",
      "원본행번호",
      "처리일시",
      "담당자",
      "위험도",
      "원본상담내용",
      "원본제공서비스",
      "Gemini프롬프트"
    ],
    monthly: [
      "실적월",
      "신규접수",
      "병원연계",
      "시설연계",
      "주거지원",
      "물품제공",
      "방문상담",
      "외래진료",
      "투약관리",
      "공적서비스",
      "기타서비스",
      "총 서비스제공"
    ],
    error: [
      "기록일시",
      "고유번호",
      "오류유형",
      "오류메시지",
      "입력이름",
      "입력날짜",
      "상태"
    ]
  },
  requiredSourceHeaders: ["공통_날짜", "공통_이름"],
  meaningfulKeys: [
    "공통_번호",
    "공통_날짜",
    "공통_담당자",
    "공통_이름",
    "병원_입원유형",
    "병원_병원명",
    "병원_입원여부",
    "병원_퇴원후연계",
    "시설_입소유형",
    "시설_시설명",
    "시설_입퇴소여부",
    "시설_퇴소후조치",
    "서비스_병원명",
    "서비스_주거지원",
    "서비스_물품제공",
    "서비스_방문상담",
    "서비스_외래진료",
    "서비스_투약관리",
    "공적서비스_지원항목",
    "공적서비스_신청기관",
    "공적서비스_지원시기",
    "서비스_기타"
  ],
  serviceFieldMap: [
    { key: "서비스_주거지원", type: "주거지원" },
    { key: "서비스_물품제공", type: "물품제공" },
    { key: "서비스_방문상담", type: "방문상담" },
    { key: "서비스_외래진료", type: "외래진료" },
    { key: "서비스_투약관리", type: "투약관리" }
  ],
  rawInputTemplate: {
    groupHeaders: [
      "공통", "공통", "공통", "공통", "공통", "공통", "공통",
      "병원", "병원", "병원", "병원",
      "시설", "시설", "시설", "시설",
      "서비스", "서비스", "서비스", "서비스", "서비스", "서비스", "서비스",
      "공적서비스", "공적서비스", "공적서비스"
    ],
    detailHeaders: [
      "번호", "날짜", "담당자", "이름", "생년월일", "성별", "상담유형",
      "입원유형", "병원명", "입원여부", "퇴원후연계",
      "입소유형", "시설명", "입퇴소여부", "퇴소후조치",
      "병원명", "주거지원", "물품제공", "방문상담", "외래진료", "투약관리", "기타",
      "지원항목", "신청기관", "지원시기"
    ],
    checkboxFields: [
      "서비스_주거지원",
      "서비스_물품제공",
      "서비스_방문상담",
      "서비스_외래진료",
      "서비스_투약관리"
    ],
    columnWidths: [
      90, 100, 100, 100, 110, 70, 100,
      110, 160, 100, 170,
      110, 160, 100, 170,
      150, 90, 90, 90, 90, 90, 180,
      150, 150, 120
    ]
  },
  settingsOptions: [
    {
      section: "운영설정",
      key: "raw_input_sheet_name",
      defaultValue: "원본데이터",
      description: "원본 입력 시트 이름입니다. 없으면 지정이름, 시작이 raw_input, 시트1 등을 순서대로 탐색합니다."
    },
    {
      section: "운영설정",
      key: "rebuild_mode",
      defaultValue: "full_rebuild",
      description: "1차 운영은 전체 재구성 방식으로 유지합니다."
    },
    {
      section: "운영설정",
      key: "skip_invalid_detail_rows",
      defaultValue: "TRUE",
      description: "필수값 누락 또는 날짜 오류 행은 상세 시트 생성에서 제외합니다."
    },
    {
      section: "필수값",
      key: "required_fields",
      defaultValue: "날짜,이름",
      description: "최소 필수값 기준입니다."
    },
    {
      section: "AI상담기록",
      key: "counseling_ai_source_sheet_name",
      defaultValue: "",
      description: "AI 상담기록 원본 시트 이름입니다. 비어 있으면 현재 활성 시트, 2601/2602/2603류 시트, 원본데이터 순으로 탐색합니다."
    },
    {
      section: "AI상담기록",
      key: "counseling_ai_output_sheet_name",
      defaultValue: "AI상담기록",
      description: "AI가 작성한 상담기록 결과 시트 이름입니다."
    },
    {
      section: "AI상담기록",
      key: "counseling_ai_record_format",
      defaultValue: "일일상담기록",
      description: "AI 출력 형식입니다. 일일상담기록 또는 사례정리보고서로 설정합니다."
    },
    {
      section: "AI상담기록",
      key: "counseling_ai_header_rows",
      defaultValue: "",
      description: "원본 시트의 헤더 행 수입니다. 비워두면 자동 감지하며, 그룹 헤더 + 상세 헤더 구조라면 2로 설정합니다."
    },
    {
      section: "AI상담기록",
      key: "counseling_ai_model",
      defaultValue: "Google Sheets =GEMINI()",
      description: "AI입력텍스트 생성 방식 표기용 값입니다."
    },
    {
      section: "AI상담기록",
      key: "counseling_ai_batch_size",
      defaultValue: "10",
      description: "미처리 행 일괄 생성 시 한 번에 처리할 최대 행 수입니다."
    },
    {
      section: "AI상담기록",
      key: "counseling_ai_skip_completed",
      defaultValue: "TRUE",
      description: "이미 생성 완료된 원본행은 기본적으로 건너뜁니다."
    }
  ],
  settingsCodeGroups: [
    {
      name: "상담유형",
      values: [{ value: "정신" }, { value: "알코올" }, { value: "기타" }]
    },
    {
      name: "입원유형",
      values: [
        { value: "자의입원" },
        { value: "동의입원" },
        { value: "행정입원" },
        { value: "응급입원" },
        { value: "기타" }
      ]
    },
    {
      name: "입원여부",
      values: [{ value: "입원" }, { value: "퇴원" }, { value: "기타" }]
    },
    {
      name: "입소유형",
      values: [{ value: "자활" }, { value: "재활" }, { value: "요양" }, { value: "일시보호" }, { value: "기타" }]
    },
    {
      name: "입퇴소여부",
      values: [{ value: "입소" }, { value: "퇴소" }, { value: "기타" }]
    },
    {
      name: "서비스유형",
      values: [
        { value: "주거지원" },
        { value: "물품제공" },
        { value: "방문상담" },
        { value: "외래진료" },
        { value: "투약관리" },
        { value: "기타" }
      ]
    }
  ],
  dashboard: {
    lookupCell: "B28"
  },
  counselingAi: {
    defaultModel: "Google Sheets =GEMINI()",
    defaultRecordFormat: "일일상담기록",
    supportedRecordFormats: ["일일상담기록", "사례정리보고서"],
    interRequestDelayMs: 3500,
    minRetryDelayMs: 3000,
    maxRetryAttempts: 4,
    maxContextCharacters: 6000,
    piiPartialHeaderKeywords: [
      "이름",
      "성명",
      "주민",
      "생년월일",
      "연락처",
      "휴대폰",
      "전화",
      "주소",
      "이메일"
    ],
    piiExactHeaderKeywords: [
      "담당자",
      "작성자",
      "상담자",
      "직원명",
      "사례관리자"
    ],
    nameAliases: ["이름", "성명", "대상자", "내담자", "client", "ct"],
    dateAliases: ["상담일", "날짜", "일자", "기록일", "접수일"],
    workerAliases: ["담당자", "상담자", "작성자", "사례관리자"],
    typeAliases: ["상담유형", "유형", "분류", "구분", "상담분류"],
    noteAliases: ["상담내용", "상담메모", "기록", "메모", "특이사항", "비고", "내용", "주호소", "상세내용"],
    serviceAliases: [
      "제공서비스",
      "제공서비스1",
      "제공서비스2",
      "제공서비스3",
      "제공서비스4",
      "제공 서비스",
      "서비스제공"
    ],
    channelAliases: ["상담방법", "상담채널", "방법", "채널"],
    preferredSourceSheetNames: ["2601", "2602", "2603"],
    preferredSourceSheetKeywords: ["실적표", "260"],
    hiddenLegacySheetNames: [
      "기본정보",
      "병원기록",
      "시설기록",
      "서비스기록",
      "공적서비스",
      "월간실적",
      "오류로그",
      "요약대시보드"
    ],
    defaultRiskLevel: "정보부족"
  },
  dailyStats: {
    sheetPrefix: "", // 사용자가 "2026년 1월" 식으로 쓰길 원함. 접두어 생략.
    staffNames: ["조국일", "최리선", "안영호", "박상준", "오강현", "김영민", "임종혁", "마명철"],
    // 각 열의 데이터를 어떤 조건에서 +1 할 것인가에 대한 매핑
    mappingRules: {
      "정신상담": { sheet: "base", col: "상담유형", val: "정신" },
      "알콜상담": { sheet: "base", col: "상담유형", val: "알코올" },
      "기타상담": { sheet: "base", col: "상담유형", val: "기타" },
      "자의입원": { sheet: "hospital", col: "입원유형", val: "자의입원" },
      "응급입원": { sheet: "hospital", col: "입원유형", val: "응급입원" },
      // 모호한 매핑 조건들 (우선 논리적 추정)
      "보호진단": { sheet: "hospital", col: "입원유형", val: ["행정입원", "동의입원"] },
      "시설입소(노숙영역)": { sheet: "facility", col: "입소유형", val: ["자활", "재활", "요양", "일시보호"] },
      "시설입소(타복지영역)": { sheet: "facility", col: "시설명", contains: "타복지" },
      "지원주택": { sheet: "facility", col: "시설명", contains: "지원주택" },
      "주거지원": { sheet: "service", col: "서비스유형", val: "주거지원" },
      "물품제공": { sheet: "service", col: "서비스유형", val: "물품제공" },
      "기타(귀가,외래등)": { sheet: "hospital", col: "퇴원후연계", notEmpty: true },
      "외래진료": { sheet: "service", col: "서비스유형", val: "외래진료" },
      "병원방문": { sheet: "service", col: "서비스상세", contains: "병원방문" },
      "주거지방문": { sheet: "service", col: "서비스유형", val: "방문상담" },
      "투약관리": { sheet: "service", col: "서비스유형", val: "투약관리" },
      "기타(척도, 내방)": { sheet: "service", col: "서비스유형", val: "기타" }
    }
  }
};
