# app.py
from fastapi import FastAPI, HTTPException, Header
import os, requests, time

# ── 환경변수 ──────────────────────────────────────────────────────────────────
UP_URL   = os.environ["UPSTASH_URL"].rstrip("/")   # 예: https://xxxx.upstash.io
UP_TOKEN = os.environ["UPSTASH_TOKEN"]             # 예: AYxxxx...
API_KEY  = os.getenv("API_KEY")                    # 선택(있으면 헤더로 검증)

app = FastAPI(title="KR Snapshot API (Free)", version="1.3")

# ── 인증 (사용 시 x-api-key 헤더 필요) ───────────────────────────────────────
def check_auth(x_api_key: str | None):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Unauthorized")

# ── Upstash 호출 유틸 ────────────────────────────────────────────────────────
def upstash_pipeline(cmds: list[list[str]]):
    """
    1순위: /pipeline (본문은 반드시 JSON 배열)
    성공 시 응답 예: [{"result": ...}, ...]
    """
    r = requests.post(
        f"{UP_URL}/pipeline",
        headers={
            "Authorization": f"Bearer {UP_TOKEN}",
            "Content-Type": "application/json",
        },
        json=cmds,                 # ← 배열(JSON array) 본문 필수
        timeout=10,
    )
    if r.status_code != 200:
        raise HTTPException(502, f"Upstash {r.status_code}: {r.text}")
    return r.json()

def upstash_hgetall(key: str) -> dict:
    # ❶ pipeline 방식 시도
    try:
        data = upstash_pipeline([["HGETALL", key]])
        # 보통 [{"result":[...]}] 형식
        arr = None
        if isinstance(data, list) and data:
            arr = data[0].get("result")
        elif isinstance(data, dict):
            arr = data.get("result")
        if isinstance(arr, list):
            it = iter(arr)
            return {k: v for k, v in zip(it, it)}
    except HTTPException:
        # 그대로 폴백
        pass
    except Exception:
        pass

    # ❷ 경로(Path) 방식 폴백: GET /HGETALL/<key>
    try:
        r = requests.get(
            f"{UP_URL}/HGETALL/{key}",
            headers={"Authorization": f"Bearer {UP_TOKEN}"},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            arr = data.get("result") if isinstance(data, dict) else None
            if isinstance(arr, list):
                it = iter(arr)
                return {k: v for k, v in zip(it, it)}
    except Exception:
        pass

    # ❸ 루트+command 폴백: POST { "command": ["HGETALL", key] }
    try:
        r = requests.post(
            f"{UP_URL}",
            headers={"Authorization": f"Bearer {UP_TOKEN}", "Content-Type": "application/json"},
            json={"command": ["HGETALL", key]},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            arr = data.get("result") if isinstance(data, dict) else None
            if isinstance(arr, list):
                it = iter(arr)
                return {k: v for k, v in zip(it, it)}
    except Exception:
        pass

    # 모두 실패
    return {}

# ── 라우트 ──────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "ok": True,
        "message": "KR Snapshot API is running. Use /snapshot?ticker=005930",
        "version": "1.3"
    }

@app.get("/health")
def health():
    """
    Upstash 연결 점검: PING
    가능한 한 /pipeline 우선, 안 되면 다른 방식 시도.
    """
    # pipeline
    try:
        resp = upstash_pipeline([["PING"]])
        return {"ok": True, "method": "pipeline", "resp": resp}
    except Exception as e:
        last_err = str(e)

    # path
    try:
        r = requests.get(f"{UP_URL}/PING", headers={"Authorization": f"Bearer {UP_TOKEN}"}, timeout=10)
        if r.status_code == 200:
            return {"ok": True, "method": "path", "resp": r.json()}
        last_err = f"status {r.status_code}: {r.text}"
    except Exception as e:
        last_err = str(e)

    # command
    try:
        r = requests.post(
            f"{UP_URL}",
            headers={"Authorization": f"Bearer {UP_TOKEN}", "Content-Type": "application/json"},
            json={"command": ["PING"]},
            timeout=10,
        )
        if r.status_code == 200:
            return {"ok": True, "method": "command", "resp": r.json()}
        last_err = f"status {r.status_code}: {r.text}"
    except Exception as e:
        last_err = str(e)

    raise HTTPException(502, f"Upstash REST failed: {last_err}")

@app.get("/snapshot")
def snapshot(ticker: str, x_api_key: str | None = Header(default=None)):
    """
    최신 스냅샷 조회: GitHub Actions가 저장한 SNAP:<ticker>를 읽음.
    예: /snapshot?ticker=005930
    """
    check_auth(x_api_key)
    d = upstash_hgetall(f"SNAP:{ticker}")
    if not d:
        raise HTTPException(404, f"No snapshot for {ticker} (maybe not fetched yet)")
    return {
        "ticker": ticker,
        "serverTs": int(time.time() * 1000),
        "dataTs": int(d.get("ts", 0)),
        "price": float(d.get("price", 0)),
        "note": "polled every ~2 minutes (free plan)"
    }
