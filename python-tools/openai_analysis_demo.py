import os
from openai import OpenAI

# OpenAI API 키 설정 (환경 변수 또는 직접 입력)
# 예: os.environ["OPENAI_API_KEY"] = "YOUR_API_KEY"
api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    print("오류: OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
    print("사용법: set OPENAI_API_KEY=your_key_here (Windows CMD)")
    print("또는 스크립트 상단에 os.environ['OPENAI_API_KEY'] = 'your_key'를 추가하세요.")
else:
    client = OpenAI(api_key=api_key)

    def categorize_counseling(text):
        """
        OpenAI GPT-4o 모델을 사용하여 상담 내용을 분류합니다.
        (과거 Codex 모델의 최신 후계 모델입니다.)
        """
        prompt = f"""
        당신은 정신건강팀의 전문 상담 분류가입니다. 
        아래 상담 내용을 보고 다음 카테고리 중 하나로 분류하고 그 이유를 짧게 적어주세요.
        카테고리: [정신상담, 알콜상담, 일반상담]

        상담 내용: "{text}"
        
        형식:
        분류: [카테고리명]
        이유: [이유]
        """
        
        try:
            response = client.chat.completions.create(
                model="gpt-4o", # 가장 최신 모델 추천
                messages=[
                    {"role": "system", "content": "당신은 상담 데이터 분류 전문가입니다."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"API 호출 중 오류 발생: {e}"

    # 테스트 실행
    sample_text = "대상자가 최근 환청이 들린다고 호소하며 불안해함. 약물 복용 여부 확인 및 상담 진행."
    print(f"테스트 문구: {sample_text}")
    print("-" * 30)
    result = categorize_counseling(sample_text)
    print(result)
