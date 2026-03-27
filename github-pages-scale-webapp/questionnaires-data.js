window.__SCALE_SCREENING_BUNDLE__ = {
  "manifest": [
    {
      "id": "phq-9",
      "title": "우울(PHQ-9)",
      "recommendedAge": "만19세 이상",
      "path": "./questionnaires/phq-9.json"
    },
    {
      "id": "gad-7",
      "title": "불안(GAD-7)",
      "recommendedAge": "청소년 이상",
      "path": "./questionnaires/gad-7.json"
    },
    {
      "id": "mkpq-16",
      "title": "조기정신증(mKPQ-16)",
      "recommendedAge": "청소년 이상",
      "path": "./questionnaires/mkpq-16.json"
    },
    {
      "id": "k-mdq",
      "title": "조울증(K-MDQ)",
      "recommendedAge": "성인 이상",
      "path": "./questionnaires/k-mdq.json"
    },
    {
      "id": "pss-10",
      "title": "스트레스(PSS)",
      "recommendedAge": "청소년 이상",
      "path": "./questionnaires/pss-10.json"
    },
    {
      "id": "isi-k",
      "title": "불면(ISI-K)",
      "recommendedAge": "성인 이상",
      "path": "./questionnaires/isi-k.json"
    },
    {
      "id": "audit-k",
      "title": "알코올중독(AUDIT-K)",
      "recommendedAge": "성인 이상",
      "path": "./questionnaires/audit-k.json"
    },
    {
      "id": "ies-r",
      "title": "사건충격척도(IES-R)",
      "recommendedAge": "",
      "path": "./questionnaires/ies-r.json"
    },
    {
      "id": "cri",
      "title": "정신건강위기평정척도(CRI)",
      "recommendedAge": "",
      "path": "./questionnaires/cri.json"
    }
  ],
  "questionnaires": {
    "phq-9": {
      "id": "phq-9",
      "selfSeq": 1,
      "title": "우울(PHQ-9)",
      "shortTitle": "PHQ-9",
      "recommendedAge": "만19세 이상",
      "intro": [],
      "questionPrompt": "지난 2주간, 얼마나 자주 다음과 같은 문제들로 곤란을 겪으셨습니까?",
      "source": {
        "referencePage": "",
        "institution": "",
        "citation": "박승진, 최혜라, 최지혜, 김건우, 홍진표(2010), 한글판 우울증선별도구(Patient Health Questionnaire-9, PHQ-9)의 신뢰도와 타당도. 대한불안의학회지 6, 119-24."
      },
      "respondentFields": [
        {
          "id": "gender",
          "label": "성별",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "male",
              "label": "남"
            },
            {
              "value": "female",
              "label": "여"
            }
          ]
        },
        {
          "id": "age_group",
          "label": "연령대",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "10s",
              "label": "10대"
            },
            {
              "value": "20s",
              "label": "20대"
            },
            {
              "value": "30s",
              "label": "30대"
            },
            {
              "value": "40s",
              "label": "40대"
            },
            {
              "value": "50s",
              "label": "50대"
            },
            {
              "value": "60s",
              "label": "60대"
            }
          ]
        }
      ],
      "questions": [
        {
          "id": "q1",
          "number": "1.",
          "text": "기분이 가라앉거나 우울하거나 희망이 없다고 느꼈다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "2~6일",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "7~12일",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일",
              "score": 3
            }
          ]
        },
        {
          "id": "q2",
          "number": "2.",
          "text": "평소 하던 일에 대한 흥미가 없어지거나 즐거움을 느끼지 못했다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "2~6일",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "7~12일",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일",
              "score": 3
            }
          ]
        },
        {
          "id": "q3",
          "number": "3.",
          "text": "잠들기가 어렵거나 자주 깼다. / 혹은 너무 많이 잤다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "2~6일",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "7~12일",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일",
              "score": 3
            }
          ]
        },
        {
          "id": "q4",
          "number": "4.",
          "text": "평소보다 식욕이 줄었다. / 혹은 평소보다 많이 먹었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "2~6일",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "7~12일",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일",
              "score": 3
            }
          ]
        },
        {
          "id": "q5",
          "number": "5.",
          "text": "다른 사람들이 눈치 챌 정도로 평소보다 말과 행동이 느려졌다. / 혹은 너무 안절부절 못해서 가만히 앉아있을 수 없다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "2~6일",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "7~12일",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일",
              "score": 3
            }
          ]
        },
        {
          "id": "q6",
          "number": "6.",
          "text": "피곤하고 기운이 없었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "2~6일",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "7~12일",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일",
              "score": 3
            }
          ]
        },
        {
          "id": "q7",
          "number": "7.",
          "text": "내가 잘못했거나, 실패했다는 생각이 들었다. / 혹은 자신과 가족을 실망시켰다고 생각했다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "2~6일",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "7~12일",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일",
              "score": 3
            }
          ]
        },
        {
          "id": "q8",
          "number": "8.",
          "text": "신문을 읽거나 TV를 보는 것과 같은 일상적인 일에도 집중할 수가 없었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "2~6일",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "7~12일",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일",
              "score": 3
            }
          ]
        },
        {
          "id": "q9",
          "number": "9.",
          "text": "차라리 죽는 것이 더 낫겠다고 생각했다. / 혹은 자해할 생각을 했다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "2~6일",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "7~12일",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일",
              "score": 3
            }
          ]
        }
      ],
      "scoring": {
        "type": "sum",
        "bands": [
          {
            "min": 0,
            "max": 4,
            "label": "낮음",
            "description": "우울 증상 수준이 낮은 편입니다."
          },
          {
            "min": 5,
            "max": 9,
            "label": "경도",
            "description": "가벼운 우울 증상이 시사됩니다."
          },
          {
            "min": 10,
            "max": 14,
            "label": "중등도",
            "description": "중등도 수준의 우울 증상이 시사됩니다."
          },
          {
            "min": 15,
            "max": 19,
            "label": "중증도-중등",
            "description": "임상적 평가가 필요한 수준의 우울 증상이 시사됩니다."
          },
          {
            "min": 20,
            "max": 27,
            "label": "고도",
            "description": "높은 수준의 우울 증상이 시사됩니다."
          }
        ],
        "suicideFlagQuestionId": "q9",
        "notes": [
          "PHQ-9 총점은 참고용이며 진단을 대신하지 않습니다.",
          "9번 문항이 1점 이상이면 즉시 추가 면담과 안전 확인이 필요합니다."
        ]
      },
      "extractionNotes": [
        "공개된 참고 페이지 HTML을 바탕으로 구조화했습니다.",
        "공통 헤더, 푸터, 만족도조사, CMS 내부 hidden input 값은 앱에서 제외했습니다."
      ]
    },
    "gad-7": {
      "id": "gad-7",
      "selfSeq": 2,
      "title": "불안(GAD-7)",
      "shortTitle": "GAD-7",
      "recommendedAge": "청소년 이상",
      "intro": [],
      "questionPrompt": "지난 2주 동안 당신은 다음의 문제들로 인해서 얼마나 자주 방해를 받았습니까?",
      "source": {
        "referencePage": "",
        "institution": "",
        "citation": "Spitzer RL, Kroenke K, Williams JBW, Lowe B. A brief measure for assessing generalized anxiety disorder. Arch Inern Med. 2006;166:1092-1097."
      },
      "respondentFields": [
        {
          "id": "gender",
          "label": "성별",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "male",
              "label": "남"
            },
            {
              "value": "female",
              "label": "여"
            }
          ]
        },
        {
          "id": "age_group",
          "label": "연령대",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "10s",
              "label": "10대"
            },
            {
              "value": "20s",
              "label": "20대"
            },
            {
              "value": "30s",
              "label": "30대"
            },
            {
              "value": "40s",
              "label": "40대"
            },
            {
              "value": "50s",
              "label": "50대"
            },
            {
              "value": "60s",
              "label": "60대"
            }
          ]
        }
      ],
      "questions": [
        {
          "id": "q1",
          "number": "1.",
          "text": "초조하거나 불안하거나 조마조마하게 느낀다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 방해 받지 않았다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "며칠 동안 방해 받았다.",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "2주 중 절반이상 방해 받았다.",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일 방해 받았다.",
              "score": 3
            }
          ]
        },
        {
          "id": "q2",
          "number": "2.",
          "text": "걱정하는 것을 멈추거나 조절할 수가 없다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 방해 받지 않았다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "며칠 동안 방해 받았다.",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "2주 중 절반이상 방해 받았다.",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일 방해 받았다.",
              "score": 3
            }
          ]
        },
        {
          "id": "q3",
          "number": "3.",
          "text": "여러 가지 것들에 대해 걱정을 너무 많이 한다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 방해 받지 않았다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "며칠 동안 방해 받았다.",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "2주 중 절반이상 방해 받았다.",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일 방해 받았다.",
              "score": 3
            }
          ]
        },
        {
          "id": "q4",
          "number": "4.",
          "text": "편하게 있기가 어렵다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 방해 받지 않았다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "며칠 동안 방해 받았다.",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "2주 중 절반이상 방해 받았다.",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일 방해 받았다.",
              "score": 3
            }
          ]
        },
        {
          "id": "q5",
          "number": "5.",
          "text": "너무 안절부절 못해서 가만히 있기가 힘들다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 방해 받지 않았다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "며칠 동안 방해 받았다.",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "2주 중 절반이상 방해 받았다.",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일 방해 받았다.",
              "score": 3
            }
          ]
        },
        {
          "id": "q6",
          "number": "6.",
          "text": "쉽게 짜증이 나거나 쉽게 성을 내게 된다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 방해 받지 않았다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "며칠 동안 방해 받았다.",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "2주 중 절반이상 방해 받았다.",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일 방해 받았다.",
              "score": 3
            }
          ]
        },
        {
          "id": "q7",
          "number": "7.",
          "text": "마치 끔찍한 일이 생길 것처럼 두렵게 느껴진다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 방해 받지 않았다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "며칠 동안 방해 받았다.",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "2주 중 절반이상 방해 받았다.",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "거의 매일 방해 받았다.",
              "score": 3
            }
          ]
        }
      ],
      "scoring": {
        "type": "sum",
        "bands": [
          {
            "min": 0,
            "max": 4,
            "label": "낮음",
            "description": "불안 증상 수준이 낮은 편입니다."
          },
          {
            "min": 5,
            "max": 9,
            "label": "경도",
            "description": "가벼운 불안 증상이 시사됩니다."
          },
          {
            "min": 10,
            "max": 14,
            "label": "중등도",
            "description": "중등도 수준의 불안 증상이 시사됩니다."
          },
          {
            "min": 15,
            "max": 21,
            "label": "고도",
            "description": "높은 수준의 불안 증상이 시사됩니다."
          }
        ],
        "notes": [
          "GAD-7 총점은 참고용입니다.",
          "총점 10점 이상이면 추가 평가를 권합니다."
        ]
      },
      "extractionNotes": [
        "공개된 참고 페이지 HTML을 바탕으로 구조화했습니다.",
        "공통 헤더, 푸터, 만족도조사, CMS 내부 hidden input 값은 앱에서 제외했습니다."
      ]
    },
    "mkpq-16": {
      "id": "mkpq-16",
      "selfSeq": 3,
      "title": "조기정신증(mKPQ-16)",
      "shortTitle": "mKPQ",
      "recommendedAge": "청소년 이상",
      "intro": [],
      "questionPrompt": "지난 한 달 동안 다음 생각, 느낌, 경험이 있었는지에 대해 ‘예’ 또는 ‘아니오’에 체크해주십시오.\n   술이나 약을 복용하는 동안에만 경험하는 것은 포함하지 않습니다.\n   ‘예’라고 답한 항목에는 해당경험이 얼마나 힘드셨는지 체크해주십시오.",
      "source": {
        "referencePage": "",
        "institution": "",
        "citation": "Kim SW, Chung YC, Kang YS, Kim JK, Jang JE, Jhon M, et al. Validation of the Korean version of the 16-item Prodromal Questionnaire in a Non-Help- Seeking College Population. Psychiatry Investig 2018;15(2):111-117."
      },
      "respondentFields": [
        {
          "id": "gender",
          "label": "성별",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "male",
              "label": "남"
            },
            {
              "value": "female",
              "label": "여"
            }
          ]
        },
        {
          "id": "age_group",
          "label": "연령대",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "10s",
              "label": "10대"
            },
            {
              "value": "20s",
              "label": "20대"
            },
            {
              "value": "30s",
              "label": "30대"
            },
            {
              "value": "40s",
              "label": "40대"
            },
            {
              "value": "50s",
              "label": "50대"
            },
            {
              "value": "60s",
              "label": "60대"
            }
          ]
        }
      ],
      "questions": [
        {
          "id": "q1",
          "number": "1.",
          "text": "내가 이전에 즐겨 했던 일이 흥미가 없어진다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q1_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q2",
          "number": "2.",
          "text": "지금 경험하는 일이 마치 전에도 똑같이 일어났던 것처럼 느껴질 때가 종종 있다(데자뷰).",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q2_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q3",
          "number": "3.",
          "text": "나는 때때로 다른 사람들이 느끼지 못하는 냄새나 맛을 느낀다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q3_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q4",
          "number": "4.",
          "text": "나는 쿵, 찰칵, 쉿, 짝짝, 딸랑딸랑 거리는 등의 특이한 소리를 종종 듣는다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q4_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q5",
          "number": "5.",
          "text": "때로는 내가 경험한 상황이 실제인지 상상인지 헷갈릴 때가 있다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q5_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q6",
          "number": "6.",
          "text": "내가 다른 사람을 쳐다보거나 거울 속의 내 자신을 볼 때, 내 눈 바로 앞에서 얼굴이 바뀌는 것을 본 적이 있다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q6_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q7",
          "number": "7.",
          "text": "나는 사람을 처음 만날 때 극도로 불안해진다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q7_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q8",
          "number": "8.",
          "text": "다른 사람들이 확실히 볼 수 없는 것들을 본 적이 있다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q8_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q9",
          "number": "9.",
          "text": "내 생각들은 가끔 너무 강렬해져서 마치 실제로 내게 말하는 것처럼 느낄 때가 있다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q9_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q10",
          "number": "10.",
          "text": "나는 때때로 광고나 상품 진열대 또는 내 주변에 배치된 것들에서 특별한 의미를 발견한다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q10_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q11",
          "number": "11.",
          "text": "때때로 나는 내 아이디어나 생각을 통제하지 못한다고 느낄 때가 있다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q11_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q12",
          "number": "12.",
          "text": "때때로 나는 평소에는 알아채지 못했을 희미한 소리에 갑자기 정신이 산만해 진다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q12_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q13",
          "number": "13.",
          "text": "다른 사람들이 듣지 못하는 속삼임이나 말소리를 들은 적이 있다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q13_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q14",
          "number": "14.",
          "text": "나는 다른 사람들이 내게 앙심을 품고 있다고 느낀 적이 종종 있다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q14_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "약간",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q15",
          "number": "15.",
          "text": "보이진 않지만, 어떤 사람이나 힘이 내 주위에 존재하는 느낌을 받은 적이 있다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q15_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q16",
          "number": "16.",
          "text": "나는 내 몸의 일부가 어떤 면에서 달라졌거나, 이전과 다르게 움직이고 있다고 느낀다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q16_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q17",
          "number": "17.",
          "text": "가끔 나에 대한 음모(모함)가 있다는 느낌이 든다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q17_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q18",
          "number": "18.",
          "text": "사람들이 말을 길게 하면 말뜻을 정확히 이해하기 어렵다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q18_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        },
        {
          "id": "q19",
          "number": "19.",
          "text": "그럴 리가 없는데 가끔 어떤 사건이나 방송들이 나와 관련이 있는 것 같다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q19_sub1",
              "number": "Q1.",
              "label": "얼마나 힘드셨는지 체크해주십시오",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "없다",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "약간",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "보통",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심함",
                  "score": 3
                }
              ]
            }
          ]
        }
      ],
      "scoring": {
        "type": "endorsement_with_distress",
        "summaryLabel": "예 응답 수와 힘듦 정도 요약",
        "referenceCutoff": "문헌상 16문항 버전 cut-off 7점 참고",
        "referenceWarning": "현재 공식 selfCare 페이지는 2026-03-24 기준 19개 표시 문항 구조라서, 16문항 절단값을 그대로 적용하기에는 한계가 있습니다.",
        "notes": [
          "예 응답 수와 힘듦 보조합계를 함께 보고 면담 판단에 활용하세요.",
          "양성 의심 시 정신건강의학과 또는 조기정신증 평가 연계가 필요합니다."
        ]
      },
      "extractionNotes": [
        "공개된 참고 페이지 HTML을 바탕으로 구조화했습니다.",
        "공통 헤더, 푸터, 만족도조사, CMS 내부 hidden input 값은 앱에서 제외했습니다."
      ]
    },
    "k-mdq": {
      "id": "k-mdq",
      "selfSeq": 4,
      "title": "조울증(K-MDQ)",
      "shortTitle": "K-MDQ",
      "recommendedAge": "성인 이상",
      "intro": [],
      "questionPrompt": "과거에 있었던 기분의 변화를 조사하는 질문입니다.( 현재 상태를 평가하는 것이 아닙니다.)",
      "source": {
        "referencePage": "",
        "institution": "",
        "citation": "전덕인, 윤보현, 정한용, 하규섭, 신영철, 박원명(2005). 한국형 기분장애 질문지의 타당화 연구. 대한신경정신의학회지, 44(5), 583-590."
      },
      "respondentFields": [
        {
          "id": "gender",
          "label": "성별",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "male",
              "label": "남"
            },
            {
              "value": "female",
              "label": "여"
            }
          ]
        },
        {
          "id": "age_group",
          "label": "연령대",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "10s",
              "label": "10대"
            },
            {
              "value": "20s",
              "label": "20대"
            },
            {
              "value": "30s",
              "label": "30대"
            },
            {
              "value": "40s",
              "label": "40대"
            },
            {
              "value": "50s",
              "label": "50대"
            },
            {
              "value": "60s",
              "label": "60대"
            }
          ]
        }
      ],
      "questions": [
        {
          "id": "q1",
          "number": "1.",
          "text": "다음처럼 당신은 평소의 자신과는 달랐던 적이 과거(예전)에 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q2",
          "number": "2.",
          "text": "기분이 너무 좋거나 들떠서 다른 사람들이 평소의 당신 모습이 아니다라고 한 적이 있었다. 또는 너무 들떠서 문제가 생긴 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q3",
          "number": "3.",
          "text": "지나치게 흥분하여 사람들에게 소리를 지르거나, 싸우거나 말다툼을 한 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q4",
          "number": "4.",
          "text": "평소보다 더욱 자신감에 찬 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q5",
          "number": "5.",
          "text": "평소보다 더욱 잠을 덜 잤거나, 또는 잠잘 필요를 느끼지 않은 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q6",
          "number": "6.",
          "text": "평소보다 말이 더 많았거나 말이 매우 빨라졌던 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q7",
          "number": "7.",
          "text": "생각이 머리 속에서 빠르게 돌아가는 것처럼 느꼈거나 마음을 차분하게 하지 못한 적이 있다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q8",
          "number": "8.",
          "text": "주위에서 벌어지는 일로 쉽게 방해 받았기 때문에, 하던 일에 집중하기 어려웠거나 할 일을 계속하지 못한 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q9",
          "number": "9.",
          "text": "평소보다 더욱 에너지가 넘쳤던 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q10",
          "number": "10.",
          "text": "평소보다 더욱 활동적이었거나 더 많은 일을 하였던 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q11",
          "number": "11.",
          "text": "평소보다 더욱 사교적이거나 적극적(외향적)이었던 적이 있었다.(하나의 예를 들면, 한밤중에 친구들에게 전화를 했다.)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q12",
          "number": "12.",
          "text": "평소보다 더욱 성행위에 관심이 간 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q13",
          "number": "13.",
          "text": "평소의 당신과는 맞지 않는 행동을 했거나, 남들이 생각하기에 지나치거나 바보 같거나 또는 위험한 행동을 한 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ]
        },
        {
          "id": "q14",
          "number": "14.",
          "text": "돈쓰는 문제로 자신이나 가족을 곤경에 빠뜨린 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "예",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니오",
              "score": 0
            }
          ],
          "subQuestions": [
            {
              "id": "q14_sub1",
              "number": "Q1.",
              "label": "만약 위의 질문 중에서 하나 이상 예라고 했다면, 그 중 몇 가지는 같은 시기에 벌어진 것입니까?",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "예",
                  "score": 1
                },
                {
                  "value": "option_2",
                  "label": "아니오",
                  "score": 0
                }
              ]
            },
            {
              "id": "q14_sub2",
              "number": "Q2.",
              "label": "이러한 일들로 인해서 어느 정도의 문제가 발생했습니까?",
              "required": true,
              "options": [
                {
                  "value": "option_1",
                  "label": "문제 없었다.",
                  "score": 0
                },
                {
                  "value": "option_2",
                  "label": "경미한 문제",
                  "score": 1
                },
                {
                  "value": "option_3",
                  "label": "중등도의 문제",
                  "score": 2
                },
                {
                  "value": "option_4",
                  "label": "심각한 문제",
                  "score": 3
                }
              ]
            }
          ]
        }
      ],
      "scoring": {
        "type": "k_mdq",
        "symptomQuestionIds": [
          "q2",
          "q3",
          "q4",
          "q5",
          "q6",
          "q7",
          "q8",
          "q9",
          "q10",
          "q11",
          "q12",
          "q13",
          "q14"
        ],
        "samePeriodQuestionId": "q14_sub1",
        "impairmentQuestionId": "q14_sub2",
        "positiveRule": {
          "symptomYesAtLeast": 7,
          "samePeriodAtLeast": 1,
          "impairmentAtLeast": 1
        },
        "notes": [
          "K-MDQ는 증상 문항 13개(q2-q14)를 기준으로 예 응답 개수를 계산합니다.",
          "참고 양성 기준은 증상 7개 이상, 같은 시기 발생 예, 기능 문제 경미 이상입니다."
        ]
      },
      "extractionNotes": [
        "공개된 참고 페이지 HTML을 바탕으로 구조화했습니다.",
        "공통 헤더, 푸터, 만족도조사, CMS 내부 hidden input 값은 앱에서 제외했습니다."
      ]
    },
    "pss-10": {
      "id": "pss-10",
      "selfSeq": 5,
      "title": "스트레스(PSS)",
      "shortTitle": "PSS",
      "recommendedAge": "청소년 이상",
      "intro": [],
      "questionPrompt": "다음의 문항들은 최근 1개월 동안 당신이 느끼고 생각한 것에 대한 것입니다.\n   각 문항의 내용을 얼마나 자주 느꼈는지 선택해 주십시오.",
      "source": {
        "referencePage": "",
        "institution": "",
        "citation": "이종하, 신철민, 고영훈, 임재형, 조숙행, 김승현, 정인과, 한창수(2012). 한글판 스트레스 자각척도의 신뢰도와 타당도 연구. 정신신체의학,20(2),127-134"
      },
      "respondentFields": [
        {
          "id": "gender",
          "label": "성별",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "male",
              "label": "남"
            },
            {
              "value": "female",
              "label": "여"
            }
          ]
        },
        {
          "id": "age_group",
          "label": "연령대",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "10s",
              "label": "10대"
            },
            {
              "value": "20s",
              "label": "20대"
            },
            {
              "value": "30s",
              "label": "30대"
            },
            {
              "value": "40s",
              "label": "40대"
            },
            {
              "value": "50s",
              "label": "50대"
            },
            {
              "value": "60s",
              "label": "60대"
            }
          ]
        }
      ],
      "questions": [
        {
          "id": "q1",
          "number": "1.",
          "text": "최근 1개월 동안, 예상치 못했던 일 때문에 당황했던 적이 얼마나 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 4
            }
          ]
        },
        {
          "id": "q2",
          "number": "2.",
          "text": "최근 1개월 동안, 인생에서 중요한 일들을 조절할 수 없다는 느낌을 얼마나 경험하였습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 4
            }
          ]
        },
        {
          "id": "q3",
          "number": "3.",
          "text": "최근 1개월 동안, 신경이 예민해지고 스트레스를 받고 있다는 느낌을 얼마나 경험하였습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 4
            }
          ]
        },
        {
          "id": "q4",
          "number": "4.",
          "text": "최근 1개월 동안, 당신의 개인적 문제들을 다루는데 있어서 얼마나 자주 자신감을 느끼셨습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 4
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 3
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 1
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 0
            }
          ]
        },
        {
          "id": "q5",
          "number": "5.",
          "text": "최근 1개월 동안, 일상의 일들이 당신의 생각대로 진행되고 있다는 느낌을 얼마나 경험하였습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 4
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 3
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 1
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 0
            }
          ]
        },
        {
          "id": "q6",
          "number": "6.",
          "text": "최근 1개월 동안, 당신이 꼭 해야 하는 일을 처리할 수 없다고 생각한 적이 얼마나 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 4
            }
          ]
        },
        {
          "id": "q7",
          "number": "7.",
          "text": "최근 1개월 동안, 일상생활의 짜증을 얼마나 자주 잘 다스릴 수 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 4
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 3
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 1
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 0
            }
          ]
        },
        {
          "id": "q8",
          "number": "8.",
          "text": "최근 1개월 동안, 최상의 컨디션이라고 얼마나 자주 느끼셨습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 4
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 3
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 1
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 0
            }
          ]
        },
        {
          "id": "q9",
          "number": "9.",
          "text": "최근 1개월 동안, 당신이 통제할 수 없는 일 때문에 화가 난 경험이 얼마나 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 4
            }
          ]
        },
        {
          "id": "q10",
          "number": "10.",
          "text": "최근 1개월 동안, 어려운 일들이 너무 많이 쌓여서 극복하지 못할 것 같은 느낌을 얼마나 자주 경험하셨습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "거의없음",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "때때로 있음",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "자주있음",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주있음",
              "score": 4
            }
          ]
        }
      ],
      "scoring": {
        "type": "sum",
        "bands": [
          {
            "min": 0,
            "max": 13,
            "label": "낮음",
            "description": "지각된 스트레스 수준이 낮은 편입니다."
          },
          {
            "min": 14,
            "max": 26,
            "label": "중간",
            "description": "중간 수준의 스트레스가 시사됩니다."
          },
          {
            "min": 27,
            "max": 40,
            "label": "높음",
            "description": "높은 스트레스 수준이 시사됩니다."
          }
        ],
        "notes": [
          "원본 selfCare 페이지는 긍정 문항 역채점을 이미 count 값에 반영하고 있어 그대로 합산합니다.",
          "PSS는 참고용 스트레스 지표이며 단독 진단 도구는 아닙니다."
        ]
      },
      "extractionNotes": [
        "공개된 참고 페이지 HTML을 바탕으로 구조화했습니다.",
        "공통 헤더, 푸터, 만족도조사, CMS 내부 hidden input 값은 앱에서 제외했습니다."
      ]
    },
    "isi-k": {
      "id": "isi-k",
      "selfSeq": 6,
      "title": "불면(ISI-K)",
      "shortTitle": "ISI-K",
      "recommendedAge": "성인 이상",
      "intro": [],
      "questionPrompt": "아래에 문항을 잘 읽으신 후, 지난 2주 \n동안 당신의 수면상태를 가장 잘 나타낸다고 생각되는 항목을 선택하여 주십시오. 한 문항도 빠짐없이 선택해 주시기 바랍니다.",
      "source": {
        "referencePage": "",
        "institution": "",
        "citation": "Yong Won Cho, Mei Ling Song, Charies M. Morin(2014). Validation of a Korean Version of the Insomnia Severity Index. J Clin Neurol 10(3), 210-215"
      },
      "respondentFields": [
        {
          "id": "gender",
          "label": "성별",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "male",
              "label": "남"
            },
            {
              "value": "female",
              "label": "여"
            }
          ]
        },
        {
          "id": "age_group",
          "label": "연령대",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "10s",
              "label": "10대"
            },
            {
              "value": "20s",
              "label": "20대"
            },
            {
              "value": "30s",
              "label": "30대"
            },
            {
              "value": "40s",
              "label": "40대"
            },
            {
              "value": "50s",
              "label": "50대"
            },
            {
              "value": "60s",
              "label": "60대"
            }
          ]
        }
      ],
      "questions": [
        {
          "id": "q1",
          "number": "1-1.",
          "text": "잠들기 어려움",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간 있음",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "중간",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "심함",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 심함",
              "score": 4
            }
          ]
        },
        {
          "id": "q2",
          "number": "1-2.",
          "text": "잠을 유지하기 어려움(자주 깸)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간 있음",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "중간",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "심함",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 심함",
              "score": 4
            }
          ]
        },
        {
          "id": "q3",
          "number": "1-3.",
          "text": "새벽에 너무 일찍 잠에서 깸",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간 있음",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "중간",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "심함",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 심함",
              "score": 4
            }
          ]
        },
        {
          "id": "q4",
          "number": "2.",
          "text": "현재 수면패턴에 얼마나 만족하십니까 ?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "매우 만족함",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "만족함",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "중간",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "불만족",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 불만족",
              "score": 4
            }
          ]
        },
        {
          "id": "q5",
          "number": "3.",
          "text": "불면증으로 인한 삶의 질 손상 정도가 다른 사람들에게 어떻게 보인다고 생각합니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 현저하지 않음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "조금 현저함",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "다소 현저함",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이 현저함",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 많이 현저함",
              "score": 4
            }
          ]
        },
        {
          "id": "q6",
          "number": "4.",
          "text": "현재 불면증에 관하여 얼마나 걱정하고 있습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 걱정하지 않음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "조금 걱정함",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "다소 걱정함",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이 걱정함",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 많이 걱정함",
              "score": 4
            }
          ]
        },
        {
          "id": "q7",
          "number": "5.",
          "text": "당신의 수면 문제가 일상 생활(예: 낮 동안 피곤함, 업무 또는 일상적 가사능력, 집중력, 기억력, 기분, 등)을 어느 정도 방해한다고 생각합니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 방해되지 않음",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "조금 방해됨",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "다소 방해됨",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이 방해됨",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 많이 방해됨",
              "score": 4
            }
          ]
        }
      ],
      "scoring": {
        "type": "sum",
        "bands": [
          {
            "min": 0,
            "max": 7,
            "label": "정상 범위",
            "description": "임상적으로 의미 있는 불면 수준은 낮습니다."
          },
          {
            "min": 8,
            "max": 14,
            "label": "역치하 불면",
            "description": "초기 개입과 수면위생 교육을 고려할 수 있습니다."
          },
          {
            "min": 15,
            "max": 21,
            "label": "중등도 불면",
            "description": "전문 평가가 필요한 수준의 불면이 시사됩니다."
          },
          {
            "min": 22,
            "max": 28,
            "label": "중증 불면",
            "description": "적극적인 전문 개입이 권장됩니다."
          }
        ],
        "notes": [
          "ISI-K 총점은 최근 2주간의 불면 심각도 참고치입니다."
        ]
      },
      "extractionNotes": [
        "공개된 참고 페이지 HTML을 바탕으로 구조화했습니다.",
        "공통 헤더, 푸터, 만족도조사, CMS 내부 hidden input 값은 앱에서 제외했습니다."
      ]
    },
    "audit-k": {
      "id": "audit-k",
      "selfSeq": 7,
      "title": "알코올중독(AUDIT-K)",
      "shortTitle": "AUDIT-K",
      "recommendedAge": "성인 이상",
      "intro": [],
      "questionPrompt": "다음의 각 문장을 읽고, 자신에게 알맞은 답변에 체크하여 주시기 바랍니다.",
      "source": {
        "referencePage": "",
        "institution": "",
        "citation": "조근호, 채숙희, 박애란, 이해국, 신임희, 민성호(2009). 「위험 음주자의 선별을 위한 한국어판 Alcohol Use Disorders Identification Test(AUDIT-K)의 최적 절단값. J. Korean Academy of Addiction Psychiatry, 13(1), 34-40」"
      },
      "respondentFields": [
        {
          "id": "gender",
          "label": "성별",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "male",
              "label": "남"
            },
            {
              "value": "female",
              "label": "여"
            }
          ]
        },
        {
          "id": "age_group",
          "label": "연령대",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "10s",
              "label": "10대"
            },
            {
              "value": "20s",
              "label": "20대"
            },
            {
              "value": "30s",
              "label": "30대"
            },
            {
              "value": "40s",
              "label": "40대"
            },
            {
              "value": "50s",
              "label": "50대"
            },
            {
              "value": "60s",
              "label": "60대"
            }
          ]
        }
      ],
      "questions": [
        {
          "id": "q1",
          "number": "1.",
          "text": "술은 얼마나 자주 마십니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 마시지 않는다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "한달에 한 번 미만",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "한달에 2~4회",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "1주일에 2~3회",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "1주일에 4회 이상",
              "score": 4
            }
          ]
        },
        {
          "id": "q2",
          "number": "2.",
          "text": "평소 술을 마시는 날 몇잔 정도나 마십니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "1~2잔",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "3~4잔",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "5~6잔",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "7~9잔",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "10잔 이상",
              "score": 4
            }
          ]
        },
        {
          "id": "q3",
          "number": "3.",
          "text": "한 번 술을 마실 때 소주 1병 또는 맥주 4병 이상의 음주는 얼마나 자주 하십니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "한달에 1번 미만",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "한달에 한 번",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "일주일에 한 번",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매일 같이",
              "score": 4
            }
          ]
        },
        {
          "id": "q4",
          "number": "4.",
          "text": "지난 1년간 술을 한 번 마시기 시작하면 멈출 수 없다는 것을 안 때가 얼마나 자주 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "한달에 1번 미만",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "한달에 한 번",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "일주일에 한 번",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매일 같이",
              "score": 4
            }
          ]
        },
        {
          "id": "q5",
          "number": "5.",
          "text": "지난 1년간 평소 같으면 할 수 있었던 일을 음주 때문에 실패한 적이 얼마나 자주 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "한달에 1번 미만",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "한달에 한 번",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "일주일에 한 번",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매일 같이",
              "score": 4
            }
          ]
        },
        {
          "id": "q6",
          "number": "6.",
          "text": "지난 1년간 술을 마신 다음날 아침에 일을 나가기 위해 다시 해장술이 필요했던 적이 얼마나 자주 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "한달에 1번 미만",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "한달에 한 번",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "일주일에 한 번",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매일 같이",
              "score": 4
            }
          ]
        },
        {
          "id": "q7",
          "number": "7.",
          "text": "지난 1년간 음주 후에 죄책감이 들거나 후회를 한 적이 얼마나 자주 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "한달에 1번 미만",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "한달에 한 번",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "일주일에 한 번",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매일 같이",
              "score": 4
            }
          ]
        },
        {
          "id": "q8",
          "number": "8.",
          "text": "지난 1년간 음주 때문에 전날 밤에 있었던 일이 기억나지 않았던 적이 얼마나 자주 있었습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "한달에 1번 미만",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "한달에 한 번",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "일주일에 한 번",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매일 같이",
              "score": 4
            }
          ]
        },
        {
          "id": "q9",
          "number": "9.",
          "text": "음주로 인해 자신이나 다른 사람이 다친 적이 있습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없었다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "있지만, 지난 1년간에는 없었다.",
              "score": 2
            },
            {
              "value": "option_3",
              "label": "지난 1년간 있었다.",
              "score": 4
            }
          ]
        },
        {
          "id": "q10",
          "number": "10.",
          "text": "친척이나 친구, 의사가 당신이 술 마시는 것을 걱정 하거나 술 끊기를 권유한 적이 있습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "없었다.",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "있지만, 지난 1년간에는 없었다.",
              "score": 2
            },
            {
              "value": "option_3",
              "label": "지난 1년간 있었다.",
              "score": 4
            }
          ]
        }
      ],
      "scoring": {
        "type": "sum",
        "bands": [
          {
            "min": 0,
            "max": 7,
            "label": "낮은 위험",
            "description": "위험 음주 가능성이 낮은 편입니다."
          },
          {
            "min": 8,
            "max": 15,
            "label": "위험 음주",
            "description": "위험 음주 가능성이 시사됩니다."
          },
          {
            "min": 16,
            "max": 19,
            "label": "해로운 음주",
            "description": "해로운 음주 수준이 의심됩니다."
          },
          {
            "min": 20,
            "max": 40,
            "label": "의존 위험",
            "description": "알코올 의존 수준의 위험이 시사됩니다."
          }
        ],
        "notes": [
          "AUDIT-K 총점은 참고용 선별 결과입니다.",
          "음주 관련 기능 손상이나 안전 문제가 있으면 점수와 무관하게 즉시 상담을 권합니다."
        ]
      },
      "extractionNotes": [
        "공개된 참고 페이지 HTML을 바탕으로 구조화했습니다.",
        "공통 헤더, 푸터, 만족도조사, CMS 내부 hidden input 값은 앱에서 제외했습니다."
      ]
    },
    "ies-r": {
      "id": "ies-r",
      "selfSeq": 8,
      "title": "사건충격척도(IES-R)",
      "shortTitle": "IES-R",
      "recommendedAge": "",
      "intro": [],
      "questionPrompt": "아래의 문항은 스트레스를 주는 사건을 경험한 후 사람들이 겪는 어려움에 관한 것 입니다. 사건과 관련하여 지난 7일 동안 얼마나 많이 경험했는지 각각 문항에 선택해주세요.",
      "source": {
        "referencePage": "",
        "institution": "",
        "citation": "은헌정 등(2005). 한국판 사건충격척도 수정판의 신뢰도 및 타당도 연구. 신경정신의학회지, 44권, 3호, 303-310."
      },
      "respondentFields": [
        {
          "id": "gender",
          "label": "성별",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "male",
              "label": "남"
            },
            {
              "value": "female",
              "label": "여"
            }
          ]
        },
        {
          "id": "age_group",
          "label": "연령대",
          "type": "single_choice",
          "required": true,
          "options": [
            {
              "value": "10s",
              "label": "10대"
            },
            {
              "value": "20s",
              "label": "20대"
            },
            {
              "value": "30s",
              "label": "30대"
            },
            {
              "value": "40s",
              "label": "40대"
            },
            {
              "value": "50s",
              "label": "50대"
            },
            {
              "value": "60s",
              "label": "60대"
            }
          ]
        }
      ],
      "questions": [
        {
          "id": "q1",
          "number": "1.",
          "text": "그 사건을 상기시켜 주는 것들이 그 사건에 대한 감정(느낌)들을 다시 되살아나게 한다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간 있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이 있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q2",
          "number": "2.",
          "text": "나는 수면을 지속하는데 어려움이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간 있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이 있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q3",
          "number": "3.",
          "text": "나는 다른 일들로 인해 그 사건을 생각하게 된다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀 없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간 있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이 있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q4",
          "number": "4.",
          "text": "나는 그 사건 이후로 예민하고 화가 난다고 느꼈다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q5",
          "number": "5.",
          "text": "그 사건에 대해 생각하거나 떠오를 때마다 혼란스러워 지기 때문에 회피하려고 했다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q6",
          "number": "6.",
          "text": "내가 생각하지 않으려고 해도 그 사건이 생각난다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q7",
          "number": "7.",
          "text": "그 사건이 일어나지 않았거나, 현실이 아닌 것처럼 느꼈다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q8",
          "number": "8.",
          "text": "그 사건들을 상기시키는 것을 멀리하며 지냈다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q9",
          "number": "9.",
          "text": "그 사건의 영상이 나의 마음속에 갑자기 떠오르곤 했다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q10",
          "number": "10.",
          "text": "나는 신경이 예민해졌고 쉽게 깜짝 놀랐다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q11",
          "number": "11.",
          "text": "그 사건에 관해 생각하지 않기 위해 노력했다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q12",
          "number": "12.",
          "text": "나는 그 사건에 관하여 여전히 많은 감정을 가지고 있다는 것을 알지만 신경 쓰고 싶지 않았다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q13",
          "number": "13.",
          "text": "그 사건에 대한 나의 감정은 무감각한 느낌이었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q14",
          "number": "14.",
          "text": "나는 마치 사건 당시로 돌아간 것처럼 느끼거나 행동할 때가 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q15",
          "number": "15.",
          "text": "나는 그 사건이후로 잠들기가 어려웠다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q16",
          "number": "16.",
          "text": "나는 그 사건에 대한 강한 감정이 물 밀 듯 밀려오는 것을 느꼈다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q17",
          "number": "17.",
          "text": "내 기억에서 그 사건을 지워 버리려고 노력했다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q18",
          "number": "18.",
          "text": "나는 집중하는데 어려움이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q19",
          "number": "19.",
          "text": "그 사건을 떠올리게 하는 어떤 것에도 식은 땀, 호흡곤란, 오심, 심장 두근거림 같은 신체적인 반응을 일으켰다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q20",
          "number": "20.",
          "text": "나는 그 사건에 관한 꿈들을 꾼 적이 있었다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q21",
          "number": "21.",
          "text": "내가 주위를 경계하고 감시하고 있다고 느꼈다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        },
        {
          "id": "q22",
          "number": "22.",
          "text": "나는 그 사건에 대해 이야기 하지 않으려고 노력했다.",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "전혀없다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "약간있다",
              "score": 1
            },
            {
              "value": "option_3",
              "label": "상당히 있다",
              "score": 2
            },
            {
              "value": "option_4",
              "label": "많이있다",
              "score": 3
            },
            {
              "value": "option_5",
              "label": "매우 자주 있다",
              "score": 4
            }
          ]
        }
      ],
      "scoring": {
        "type": "sum",
        "bands": [
          {
            "min": 0,
            "max": 17,
            "label": "낮음",
            "description": "외상 후 스트레스 반응이 상대적으로 낮은 편입니다."
          },
          {
            "min": 18,
            "max": 24,
            "label": "부분 위험",
            "description": "외상 후 스트레스 반응에 대한 추가 관찰이 필요합니다."
          },
          {
            "min": 25,
            "max": 88,
            "label": "고위험",
            "description": "외상 후 스트레스 반응 고위험군으로 추가 평가가 권장됩니다."
          }
        ],
        "notes": [
          "self_seq=8은 2026-03-24 확인 기준 사건충격척도(IES-R) 페이지로 응답됩니다.",
          "총점 25점 이상은 고위험 참고선으로 활용할 수 있습니다."
        ]
      },
      "extractionNotes": [
        "공개된 참고 페이지 HTML을 바탕으로 구조화했습니다.",
        "공통 헤더, 푸터, 만족도조사, CMS 내부 hidden input 값은 앱에서 제외했습니다."
      ]
    },
    "cri": {
      "id": "cri",
      "selfSeq": 9,
      "title": "정신건강위기평정척도(CRI)",
      "shortTitle": "CRI",
      "recommendedAge": "",
      "intro": [],
      "questionPrompt": "각 항목을 확인하여 현재 위기 수준을 평가합니다.",
      "source": {
        "referencePage": "",
        "institution": "국립정신건강센터",
        "citation": "국립정신건강센터(2020). 정신건강 위기 평정 척도(CRI)."
      },
      "respondentFields": [],
      "questions": [
        {
          "id": "q1",
          "sectionTitle": "자타해 위험",
          "number": "1.",
          "text": "현재 자해/타해 폭력위험(기물파손, 욕설, 고함, 등 명백한 폭력 위험)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q2",
          "sectionTitle": "자타해 위험",
          "number": "2.",
          "text": "최근 1년 내 자살시도",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q3",
          "sectionTitle": "자타해 위험",
          "number": "3.",
          "text": "자타해 폭력 강도 혹은 치명도(경증장애수준 이상 또는 전치2주 이상)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "높다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "낮다",
              "score": 0
            }
          ]
        },
        {
          "id": "q4",
          "sectionTitle": "자타해 위험",
          "number": "4.",
          "text": "고의성 혹은 계획성 수준(자타해 의지, 사전준비, 일정조율, 도구확인 등)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "높다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "낮다",
              "score": 0
            }
          ]
        },
        {
          "id": "q5",
          "sectionTitle": "자타해 위험",
          "number": "5.",
          "text": "최근 1년 내 자타해 위험으로 인한 경찰개입 여부",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q6",
          "sectionTitle": "자타해 위험",
          "number": "6.",
          "text": "최근 일주일내 폭력",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q7",
          "sectionTitle": "자타해 위험",
          "number": "7.",
          "text": "과거 범죄 이력 (폭행, 성폭력, 방화, 타살 등)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q8",
          "sectionTitle": "자타해 위험",
          "number": "8.",
          "text": "과거 또는 현재 환청(또는 약물, 알코올, 마약 등)에 의한 공격성 여부",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q9",
          "sectionTitle": "정신상태",
          "number": "9.",
          "text": "현재 정신증 증상을 보입니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "그렇다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니다",
              "score": 0
            }
          ]
        },
        {
          "id": "q10",
          "sectionTitle": "정신상태",
          "number": "10.",
          "text": "현재 기분장애 증상을 보입니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "그렇다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니다",
              "score": 0
            }
          ]
        },
        {
          "id": "q11",
          "sectionTitle": "정신상태",
          "number": "11.",
          "text": "현재 조증(경조증) 증상을 보입니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "그렇다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "아니다",
              "score": 0
            }
          ]
        },
        {
          "id": "q12",
          "sectionTitle": "정신상태",
          "number": "12.",
          "text": "정신과 진단을 받거나 과거 입원한 적이 있습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q13",
          "sectionTitle": "정신상태",
          "number": "13.",
          "text": "알코올/흥분제/환각제/흡입제/대마초/마약 등의 물질관련 문제가 있습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q14",
          "sectionTitle": "정신상태",
          "number": "14.",
          "text": "정신건강문제에 대한 자각 또는 통찰력에 문제가 있습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q15",
          "sectionTitle": "기능수준",
          "number": "15.",
          "text": "의사소통(언어기능/상호소통)의 손상",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q16",
          "sectionTitle": "기능수준",
          "number": "16.",
          "text": "주의나 자각 손상(부주의, 주의산만, 물건을 잘 잃어버림, 잘 넘어지거나 부딪힘, 깜박임 등)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q17",
          "sectionTitle": "기능수준",
          "number": "17.",
          "text": "기억능력 손상(심한 건망증 포함)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q18",
          "sectionTitle": "기능수준",
          "number": "18.",
          "text": "일상적인 의사결정 손상(무엇을 먹을지, 무슨 옷을 입을지, 어떤 생일선물을 사줄지 등)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q19",
          "sectionTitle": "기능수준",
          "number": "19.",
          "text": "인지적 기능의 급격한 변화 여부",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q20",
          "sectionTitle": "기능수준",
          "number": "20.",
          "text": "일상 자조능력 상실(약물관리 포함 의식주관련)",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q21",
          "sectionTitle": "기능수준",
          "number": "21.",
          "text": "가정/대인관계/업무 적응력 손상",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 1
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 0
            }
          ]
        },
        {
          "id": "q22",
          "sectionTitle": "지지체계",
          "number": "22.",
          "text": "실제 도움과 지지를 제공하는 가족, 친구, 기타의 존재가 있습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 1
            }
          ]
        },
        {
          "id": "q23",
          "sectionTitle": "지지체계",
          "number": "23.",
          "text": "현재 도움을 주지는 않으나 제공가능한 가족, 친구, 기타의 존재가 있습니까?",
          "required": true,
          "options": [
            {
              "value": "option_1",
              "label": "있다",
              "score": 0
            },
            {
              "value": "option_2",
              "label": "없다",
              "score": 1
            }
          ]
        }
      ],
      "scoring": {
        "type": "cri",
        "groupLabels": {
          "harm": "자타해 위험",
          "mental": "정신상태",
          "function": "기능수준",
          "support": "지지체계"
        },
        "groupQuestionIds": {
          "harm": [
            "q1",
            "q2",
            "q3",
            "q4",
            "q5",
            "q6",
            "q7",
            "q8"
          ],
          "mental": [
            "q9",
            "q10",
            "q11",
            "q12",
            "q13",
            "q14"
          ],
          "function": [
            "q15",
            "q16",
            "q17",
            "q18",
            "q19",
            "q20",
            "q21"
          ],
          "support": [
            "q22",
            "q23"
          ]
        },
        "primaryRiskQuestionId": "q1",
        "psychosisRiskQuestionId": "q8",
        "grades": {
          "A": {
            "label": "A등급 (극도의 위기)",
            "description": "정신건강전문요원, 경찰, 119구급대원 공조하여 정신과 응급입원 필요",
            "flagLevel": "warn",
            "flagText": "극도의 위기 수준으로 즉시 응급평가와 안전 확보가 필요합니다."
          },
          "B": {
            "label": "B등급 (위기)",
            "description": "현재 또는 과거 자살/자해 경험이 있는 경우 정신과 외래치료 및 전문가 연계 필요(단, 단순 폭력상황이라고 판단되는 경우 경찰에 인계)",
            "flagLevel": "warn",
            "flagText": "위기 수준으로 신속한 전문기관 연계가 필요합니다."
          },
          "C": {
            "label": "C등급 (고위험)",
            "description": "현재 자해위험은 있거나 과거(최근) 자해 이력이 있고 정신과적 증상이 관찰되는 경우로 고위험군에 해당, 정신상태, 기능수준, 지지체계 결과를 고려하여 집중관리 필요",
            "flagLevel": "warn",
            "flagText": "고위험군으로 집중관찰과 추가 평가가 필요합니다."
          },
          "D": {
            "label": "D등급 (주의)",
            "description": "과거 자해위험은 있으나 현재 뚜렷한 정신과적 증상이 없는 경우, 또는 과거 자해위험은 없으나 현재 의심되는 정신과적 문제나 증상이 관찰되는 경우로 주의관찰 필요",
            "flagLevel": "info",
            "flagText": "주의관찰과 후속 확인이 필요합니다."
          },
          "E": {
            "label": "E등급 (위기 상황 아님)",
            "description": "위기상황 아님",
            "flagLevel": ""
          }
        },
        "notes": [
          "결과해석은 절대적 기준이 아니며 평가자의 종합적 판단이 가장 중요합니다.",
          "척도 개발: 국립정신건강센터, 2020"
        ]
      }
    }
  }
};
