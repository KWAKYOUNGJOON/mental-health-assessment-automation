import google.generativeai as genai
import os

# API 키 설정 (사용자가 직접 입력해야 함)
# 예: os.environ["GEMINI_API_KEY"] = "YOUR_API_KEY"
api_key = os.getenv("GEMINI_API_KEY")

if not api_key:
    print("오류: GEMINI_API_KEY 환경 변수가 설정되지 않았습니다.")
    print("사용법: set GEMINI_API_KEY=your_key_here (Windows CMD) 또는 $env:GEMINI_API_KEY='your_key_here' (PowerShell)")
else:
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    def categorize_counseling(text):
        prompt = f"""
        당신은 정신건강팀의 전문 상담 분류가입니다. 
        아래 상담 내용을 보고 다음 카테고리 중 하나로 분류하고 그 이유를 짧게 적어주세요.
        카테고리: [정신상담, 알콜상담, 일반상담]

        상담 내용: "{text}"
        
        형식:
        분류: [카테고리명]
        이유: [이유]
        """
        response = model.generate_content(prompt)
        return response.text

    # 테스트 실행
    sample_text = "대상자가 최근 환청이 들린다고 호소하며 불안해함. 약물 복용 여부 확인 및 상담 진행."
    print(f"테스트 문구: {sample_text}")
    print("-" * 30)
    result = categorize_counseling(sample_text)
    print(result)
