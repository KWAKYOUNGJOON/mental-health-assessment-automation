import argparse
import json
import sys
from pathlib import Path

import requests


ROOT_DIR = Path(__file__).resolve().parents[1]
APPS_SCRIPT_DIR = ROOT_DIR / "google-apps-script"
DEFAULT_CLASP_RC = Path.home() / ".clasprc.json"
DEFAULT_CLASP_JSON = APPS_SCRIPT_DIR / ".clasp.json"
APPS_SCRIPT_API_BASE = "https://script.googleapis.com/v1"
TOKEN_URL = "https://oauth2.googleapis.com/token"


class DeployError(Exception):
    pass


def parse_args():
    parser = argparse.ArgumentParser(
        description="Google Spreadsheet에 바운드된 Apps Script 프로젝트를 생성/업데이트합니다."
    )
    parser.add_argument(
        "--spreadsheet-id",
        required=True,
        help="대상 Google Spreadsheet ID",
    )
    parser.add_argument(
        "--script-title",
        default="정신건강팀실적자동화_AI상담기록",
        help="새로 생성할 Apps Script 프로젝트 제목",
    )
    parser.add_argument(
        "--script-id",
        default="",
        help="기존 Apps Script 프로젝트 ID. 지정하면 생성 대신 업데이트합니다.",
    )
    parser.add_argument(
        "--token-key",
        default="default",
        help="~/.clasprc.json 안에서 사용할 토큰 키. 기본값은 default",
    )
    parser.add_argument(
        "--clasprc-path",
        default=str(DEFAULT_CLASP_RC),
        help="clasp 토큰 파일 경로",
    )
    parser.add_argument(
        "--apps-script-dir",
        default=str(APPS_SCRIPT_DIR),
        help="업로드할 Apps Script 소스 폴더 경로",
    )
    parser.add_argument(
        "--write-clasp-json",
        action="store_true",
        help="성공 시 google-apps-script/.clasp.json을 새 scriptId로 갱신합니다.",
    )
    return parser.parse_args()


def load_clasp_token(clasprc_path: Path, token_key: str) -> dict:
    if not clasprc_path.exists():
      raise DeployError(f"clasp 토큰 파일을 찾을 수 없습니다: {clasprc_path}")

    payload = json.loads(clasprc_path.read_text(encoding="utf-8"))
    tokens = payload.get("tokens") or {}
    token = tokens.get(token_key)
    if not token:
        available = ", ".join(sorted(tokens.keys())) or "(없음)"
        raise DeployError(
            f"clasp 토큰 키 '{token_key}'를 찾을 수 없습니다. 사용 가능한 키: {available}"
        )
    return token


def refresh_access_token(token: dict) -> str:
    response = requests.post(
        TOKEN_URL,
        data={
            "client_id": token["client_id"],
            "client_secret": token["client_secret"],
            "refresh_token": token["refresh_token"],
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise DeployError("OAuth access token을 받지 못했습니다.")
    return access_token


def build_apps_script_files(apps_script_dir: Path) -> list[dict]:
    if not apps_script_dir.exists():
        raise DeployError(f"Apps Script 폴더를 찾을 수 없습니다: {apps_script_dir}")

    files = []
    for path in sorted(apps_script_dir.iterdir()):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        if path.suffix not in {".gs", ".html", ".json"}:
            continue
        if path.name == "package.json":
            continue

        source = path.read_text(encoding="utf-8")
        if path.name == "appsscript.json":
            files.append({
                "name": "appsscript",
                "type": "JSON",
                "source": source,
            })
        elif path.suffix == ".gs":
            files.append({
                "name": path.stem,
                "type": "SERVER_JS",
                "source": source,
            })
        elif path.suffix == ".html":
            files.append({
                "name": path.stem,
                "type": "HTML",
                "source": source,
            })

    if not files:
        raise DeployError("업로드할 Apps Script 파일을 찾지 못했습니다.")
    return files


def call_apps_script_api(method: str, url: str, access_token: str, **kwargs):
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {access_token}"
    headers.setdefault("Content-Type", "application/json")
    response = requests.request(method, url, headers=headers, timeout=60, **kwargs)
    if response.ok:
        return response

    try:
        payload = response.json()
    except ValueError:
        payload = {}
    message = (payload.get("error") or {}).get("message") or response.text

    if "User has not enabled the Apps Script API" in message:
        raise DeployError(
            "현재 Google 계정에서 Apps Script API 사용이 비활성화되어 있습니다.\n"
            "https://script.google.com/home/usersettings 에서 Apps Script API를 켠 뒤 다시 실행하세요."
        )

    raise DeployError(
        f"Apps Script API 호출 실패 ({response.status_code}): {message}"
    )


def create_bound_project(spreadsheet_id: str, script_title: str, access_token: str) -> str:
    response = call_apps_script_api(
        "POST",
        f"{APPS_SCRIPT_API_BASE}/projects",
        access_token,
        json={
            "title": script_title,
            "parentId": spreadsheet_id,
        },
    )
    payload = response.json()
    script_id = payload.get("scriptId")
    if not script_id:
        raise DeployError("Apps Script 프로젝트 생성은 되었지만 scriptId를 받지 못했습니다.")
    return script_id


def update_project_content(script_id: str, files: list[dict], access_token: str):
    call_apps_script_api(
        "PUT",
        f"{APPS_SCRIPT_API_BASE}/projects/{script_id}/content",
        access_token,
        json={"files": files},
    )


def write_clasp_json(script_id: str):
    DEFAULT_CLASP_JSON.write_text(
        json.dumps({"scriptId": script_id, "rootDir": "."}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main():
    args = parse_args()
    clasprc_path = Path(args.clasprc_path)
    apps_script_dir = Path(args.apps_script_dir)

    token = load_clasp_token(clasprc_path, args.token_key)
    access_token = refresh_access_token(token)
    files = build_apps_script_files(apps_script_dir)

    script_id = args.script_id.strip()
    created = False
    if not script_id:
        script_id = create_bound_project(args.spreadsheet_id, args.script_title, access_token)
        created = True

    update_project_content(script_id, files, access_token)

    if args.write_clasp_json:
        write_clasp_json(script_id)

    print(json.dumps({
        "ok": True,
        "created": created,
        "scriptId": script_id,
        "spreadsheetId": args.spreadsheet_id,
        "uploadedFiles": len(files),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except DeployError as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
    except requests.HTTPError as error:
        print(f"HTTP 오류: {error}", file=sys.stderr)
        sys.exit(1)
