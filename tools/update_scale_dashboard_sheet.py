from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path


DEFAULT_SPREADSHEET_ID = "11y5p7Cp_yN2vggMOlCwn4pKNBEmio-CmkK25Nyd2nIk"
RAW_RECORD_SHEET = "척도검사기록"
WORKER_VIEW_SHEET = "실무자보기"
RISK_VIEW_SHEET = "고위험군보기"
DASHBOARD_SHEET = "척도대시보드"
SETTINGS_SHEET = "척도설정"


def load_clasp_credentials(profile: str = "default") -> dict:
    rc_path = Path.home() / ".clasprc.json"
    data = json.loads(rc_path.read_text(encoding="utf-8"))
    tokens = data.get("tokens", {})
    if profile not in tokens:
        raise KeyError(f"clasp profile not found: {profile}")
    return tokens[profile]


def refresh_access_token(creds: dict) -> str:
    payload = urllib.parse.urlencode(
        {
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
            "refresh_token": creds["refresh_token"],
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        data = json.loads(response.read().decode("utf-8"))
    return data["access_token"]


def api_request(method: str, url: str, access_token: str, payload: dict | None = None) -> dict:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else {}


def get_spreadsheet(access_token: str, spreadsheet_id: str) -> dict:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
    return api_request("GET", url, access_token)


def batch_update(access_token: str, spreadsheet_id: str, requests: list[dict]) -> dict:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate"
    return api_request("POST", url, access_token, {"requests": requests})


def values_batch_clear(access_token: str, spreadsheet_id: str, ranges: list[str]) -> dict:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchClear"
    return api_request("POST", url, access_token, {"ranges": ranges})


def values_batch_update(access_token: str, spreadsheet_id: str, data: list[dict]) -> dict:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchUpdate"
    payload = {
        "valueInputOption": "USER_ENTERED",
        "data": data,
    }
    return api_request("POST", url, access_token, payload)


def ensure_sheet_requests(metadata: dict) -> list[dict]:
    existing_names = {sheet["properties"]["title"] for sheet in metadata.get("sheets", [])}
    sheet_specs = [
        (WORKER_VIEW_SHEET, 400, 11),
        (RISK_VIEW_SHEET, 400, 9),
        (DASHBOARD_SHEET, 400, 26),
        (SETTINGS_SHEET, 80, 3),
    ]
    requests = []
    for title, rows, columns in sheet_specs:
        if title in existing_names:
            continue
        requests.append(
            {
                "addSheet": {
                    "properties": {
                        "title": title,
                        "gridProperties": {"rowCount": rows, "columnCount": columns},
                    }
                }
            }
        )
    return requests


def build_values_payload() -> list[dict]:
    raw = f"'{RAW_RECORD_SHEET}'"
    worker = f"'{WORKER_VIEW_SHEET}'"
    worker_formula = (
        "=IFERROR(SORT(FILTER({"
        + raw + "!I2:I,"
        + raw + "!P2:P,"
        + raw + "!Q2:Q,"
        + raw + "!K2:K,"
        + "IF(" + raw + "!M2:M=\"\",,IFERROR(VALUE(REGEXEXTRACT(" + raw + "!M2:M,\"-?\\d+(?:\\.\\d+)?\")),)),"
        + raw + "!M2:M,"
        + raw + "!N2:N,"
        + raw + "!O2:O,"
        + raw + "!Y2:Y,"
        + raw + "!AA2:AA,"
        + raw + "!A2:A"
        + "},"
        + raw + "!A2:A<>\"\"),1,FALSE),\"\")"
    )
    risk_formula = (
        "=IFERROR(SORT(FILTER({"
        + worker + "!A2:A,"
        + worker + "!B2:B,"
        + worker + "!C2:C,"
        + worker + "!D2:D,"
        + worker + "!F2:F,"
        + worker + "!G2:G,"
        + worker + "!H2:H,"
        + worker + "!J2:J,"
        + worker + "!K2:K"
        + "},"
        + worker + "!K2:K<>\"\","
        + "(( " + worker + "!J2:J<>\"\" )+REGEXMATCH(" + worker + "!G2:G,\"^(A|B|C)\"))>0"
        "),1,FALSE),\"\")"
    )
    detail_formula = (
        "=IF($B$3=\"\",\"\",IFERROR(SORT(FILTER({"
        + worker + "!A2:A,"
        + worker + "!D2:D,"
        + worker + "!E2:E,"
        + worker + "!F2:F,"
        + worker + "!G2:G,"
        + worker + "!H2:H,"
        + worker + "!I2:I,"
        + worker + "!J2:J,"
        + worker + "!K2:K"
        + "},"
        + worker + "!B2:B=$B$3,"
        + "IF($E$3=\"\"," + worker + "!A2:A<>\"\","
        + worker + "!C2:C=TEXT($E$3,\"yyyy-mm-dd\"))"
        "),1,TRUE),\"검색 결과가 없습니다.\"))"
    )
    trend_formula = (
        "=IF($B$3=\"\",\"\",IFERROR(QUERY(FILTER({"
        + worker + "!A2:A,"
        + worker + "!D2:D,"
        + worker + "!E2:E,"
        + worker + "!B2:B,"
        + worker + "!C2:C"
        + "},"
        + worker + "!B2:B=$B$3,"
        + "IF($E$3=\"\"," + worker + "!A2:A<>\"\","
        + worker + "!C2:C=TEXT($E$3,\"yyyy-mm-dd\"))"
        "),\"select Col1, max(Col3) where Col3 is not null group by Col1 pivot Col2 label Col1 '검사일', max(Col3) ''\",0),\"\"))"
    )
    names_formula = "=ARRAYFORMULA(SORT(UNIQUE(FILTER(" + worker + "!B2:B," + worker + "!B2:B<>\"\"))))"

    settings_rows = [
        ["항목", "값", "설명"],
        ["기록 시트", RAW_RECORD_SHEET, "원본 검사 결과가 저장되는 시트"],
        ["문항응답 시트", "척도문항응답", "문항별 응답 원본 시트"],
        ["실무자 보기 시트", WORKER_VIEW_SHEET, "실무자가 결과를 조회하는 시트"],
        ["고위험군 보기 시트", RISK_VIEW_SHEET, "고위험 결과만 모아보는 시트"],
        ["대시보드 시트", DASHBOARD_SHEET, "대상자 검색 및 점수 변화 그래프 시트"],
        ["척도 마스터 시트", "척도마스터", "척도 메타데이터 시트"],
        ["문항 마스터 시트", "척도문항마스터", "문항 정의 시트"],
        ["선택지 마스터 시트", "척도선택지마스터", "선택지 정의 시트"],
        ["검색 사용법", "대상자명을 입력", "척도대시보드 B3에 대상자명을 입력하면 비교표와 그래프가 갱신됩니다."],
        ["생년월일 필터", "선택 입력", "동명이인 구분이 필요할 때 척도대시보드 E3에 생년월일을 입력합니다."],
    ]

    return [
        {
            "range": f"{WORKER_VIEW_SHEET}!A1:K2",
            "values": [
                ["검사일", "대상자", "생년월일", "척도", "점수값", "점수표시", "결과구간", "담당자", "비고", "경고여부", "기록고유값"],
                [worker_formula],
            ],
        },
        {
            "range": f"{RISK_VIEW_SHEET}!A1:I2",
            "values": [
                ["검사일", "대상자", "생년월일", "척도", "점수표시", "결과구간", "담당자", "경고내용", "기록고유값"],
                [risk_formula],
            ],
        },
        {
            "range": f"{SETTINGS_SHEET}!A1:C{len(settings_rows)}",
            "values": settings_rows,
        },
        {
            "range": f"{DASHBOARD_SHEET}!A1:T2",
            "values": [
                ["척도 검사 결과 대시보드"],
                ["대상자명을 입력하면 날짜별 검사 결과와 척도별 점수 변화 그래프를 확인할 수 있습니다."],
            ],
        },
        {
            "range": f"{DASHBOARD_SHEET}!A3:E5",
            "values": [
                ["대상자명", "", "", "생년월일", ""],
                ["", "", "", "", ""],
                ["검사 건수", "=IF($B$3=\"\",\"\",COUNTA(A10:A))", "", "최근 검사일", "=IF($B$3=\"\",\"\",IFERROR(MAX(A10:A),\"\"))"],
            ],
        },
        {
            "range": f"{DASHBOARD_SHEET}!A9:I10",
            "values": [
                ["검사일", "척도", "점수값", "점수표시", "결과구간", "담당자", "비고", "경고여부", "기록고유값"],
                [detail_formula],
            ],
        },
        {
            "range": f"{DASHBOARD_SHEET}!N8:T2",
            "values": [
                ["", "", "", "", "", "점수 변화 그래프 데이터", "대상자 목록"],
                ["", "", "", "", "", trend_formula, names_formula],
            ],
        },
    ]


def build_format_requests(sheet_ids: dict, metadata: dict) -> list[dict]:
    requests: list[dict] = []

    for title in (WORKER_VIEW_SHEET, RISK_VIEW_SHEET, SETTINGS_SHEET):
        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_ids[title], "gridProperties": {"frozenRowCount": 1}},
                    "fields": "gridProperties.frozenRowCount",
                }
            }
        )

    requests.append(
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_ids[DASHBOARD_SHEET], "gridProperties": {"frozenRowCount": 6}},
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )

    requests.extend(
        [
            repeat_cell_request(sheet_ids[WORKER_VIEW_SHEET], 0, 1, 0, 11, background="#d9ead3", bold=True, align="CENTER"),
            repeat_cell_request(sheet_ids[RISK_VIEW_SHEET], 0, 1, 0, 9, background="#f4cccc", bold=True, align="CENTER"),
            repeat_cell_request(sheet_ids[SETTINGS_SHEET], 0, 1, 0, 3, background="#d9ead3", bold=True, align="CENTER"),
            repeat_cell_request(sheet_ids[DASHBOARD_SHEET], 0, 1, 0, 6, background="#d9ead3", bold=True, font_size=16),
            repeat_cell_request(sheet_ids[DASHBOARD_SHEET], 2, 3, 0, 5, bold=True),
            repeat_cell_request(sheet_ids[DASHBOARD_SHEET], 4, 5, 0, 5, bold=True),
            repeat_cell_request(sheet_ids[DASHBOARD_SHEET], 8, 9, 0, 9, background="#d9ead3", bold=True, align="CENTER"),
            repeat_cell_request(sheet_ids[DASHBOARD_SHEET], 7, 8, 13, 26, background="#fff2cc", bold=True),
            {
                "mergeCells": {
                    "range": {"sheetId": sheet_ids[DASHBOARD_SHEET], "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 6},
                    "mergeType": "MERGE_ALL",
                }
            },
            {
                "setDataValidation": {
                    "range": {"sheetId": sheet_ids[DASHBOARD_SHEET], "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 1, "endColumnIndex": 2},
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_RANGE",
                            "values": [{"userEnteredValue": "=T2:T"}],
                        },
                        "inputMessage": "대상자명을 선택하거나 직접 입력하세요.",
                        "strict": False,
                        "showCustomUi": True,
                    },
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_ids[DASHBOARD_SHEET], "dimension": "COLUMNS", "startIndex": 19, "endIndex": 26},
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_ids[WORKER_VIEW_SHEET], "dimension": "COLUMNS", "startIndex": 0, "endIndex": 11},
                    "properties": {"pixelSize": 140},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_ids[RISK_VIEW_SHEET], "dimension": "COLUMNS", "startIndex": 0, "endIndex": 9},
                    "properties": {"pixelSize": 150},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_ids[SETTINGS_SHEET], "dimension": "COLUMNS", "startIndex": 0, "endIndex": 3},
                    "properties": {"pixelSize": 210},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_ids[DASHBOARD_SHEET], "dimension": "COLUMNS", "startIndex": 0, "endIndex": 9},
                    "properties": {"pixelSize": 130},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_ids[DASHBOARD_SHEET], "dimension": "COLUMNS", "startIndex": 13, "endIndex": 26},
                    "properties": {"pixelSize": 120},
                    "fields": "pixelSize",
                }
            },
        ]
    )

    dashboard_sheet = next(sheet for sheet in metadata["sheets"] if sheet["properties"]["title"] == DASHBOARD_SHEET)
    for chart in dashboard_sheet.get("charts", []):
        requests.append({"deleteEmbeddedObject": {"objectId": chart["chartId"]}})

    requests.append(build_chart_request(sheet_ids[DASHBOARD_SHEET]))
    return requests


def repeat_cell_request(sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int, background: str | None = None, bold: bool = False, align: str | None = None, font_size: int | None = None) -> dict:
    user_format: dict = {}
    fields: list[str] = []
    if background:
        user_format["backgroundColor"] = hex_to_rgb(background)
        fields.append("userEnteredFormat.backgroundColor")
    if bold:
        user_format.setdefault("textFormat", {})["bold"] = True
        fields.append("userEnteredFormat.textFormat.bold")
    if font_size:
        user_format.setdefault("textFormat", {})["fontSize"] = font_size
        fields.append("userEnteredFormat.textFormat.fontSize")
    if align:
        user_format["horizontalAlignment"] = align
        fields.append("userEnteredFormat.horizontalAlignment")

    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "cell": {"userEnteredFormat": user_format},
            "fields": ",".join(fields),
        }
    }


def build_chart_request(sheet_id: int) -> dict:
    series = []
    for column_index in range(14, 26):
        series.append(
            {
                "series": {
                    "sourceRange": {
                        "sources": [
                            {
                                "sheetId": sheet_id,
                                "startRowIndex": 8,
                                "endRowIndex": 200,
                                "startColumnIndex": column_index,
                                "endColumnIndex": column_index + 1,
                            }
                        ]
                    }
                },
                "targetAxis": "LEFT_AXIS",
            }
        )

    return {
        "addChart": {
            "chart": {
                "spec": {
                    "title": "척도별 점수 변화",
                    "basicChart": {
                        "chartType": "LINE",
                        "legendPosition": "RIGHT_LEGEND",
                        "headerCount": 1,
                        "axis": [
                            {"position": "BOTTOM_AXIS", "title": "검사일"},
                            {"position": "LEFT_AXIS", "title": "점수"},
                        ],
                        "domains": [
                            {
                                "domain": {
                                    "sourceRange": {
                                        "sources": [
                                            {
                                                "sheetId": sheet_id,
                                                "startRowIndex": 8,
                                                "endRowIndex": 200,
                                                "startColumnIndex": 13,
                                                "endColumnIndex": 14,
                                            }
                                        ]
                                    }
                                }
                            }
                        ],
                        "series": series,
                    },
                },
                "position": {
                    "overlayPosition": {
                        "anchorCell": {"sheetId": sheet_id, "rowIndex": 0, "columnIndex": 7},
                        "offsetXPixels": 0,
                        "offsetYPixels": 0,
                        "widthPixels": 840,
                        "heightPixels": 360,
                    }
                },
            }
        }
    }


def hex_to_rgb(hex_color: str) -> dict:
    normalized = hex_color.lstrip("#")
    return {
        "red": int(normalized[0:2], 16) / 255,
        "green": int(normalized[2:4], 16) / 255,
        "blue": int(normalized[4:6], 16) / 255,
    }


def main() -> None:
    spreadsheet_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SPREADSHEET_ID
    creds = load_clasp_credentials("default")
    access_token = refresh_access_token(creds)

    metadata = get_spreadsheet(access_token, spreadsheet_id)
    add_requests = ensure_sheet_requests(metadata)
    if add_requests:
        batch_update(access_token, spreadsheet_id, add_requests)
        metadata = get_spreadsheet(access_token, spreadsheet_id)

    values_batch_clear(
        access_token,
        spreadsheet_id,
        [
            f"{WORKER_VIEW_SHEET}!A:K",
            f"{RISK_VIEW_SHEET}!A:I",
            f"{SETTINGS_SHEET}!A:C",
            f"{DASHBOARD_SHEET}!A:Z",
        ],
    )
    values_batch_update(access_token, spreadsheet_id, build_values_payload())

    sheet_ids = {sheet["properties"]["title"]: sheet["properties"]["sheetId"] for sheet in metadata["sheets"]}
    batch_update(access_token, spreadsheet_id, build_format_requests(sheet_ids, metadata))
    print(json.dumps({"ok": True, "spreadsheetId": spreadsheet_id}, ensure_ascii=False))


if __name__ == "__main__":
    main()
