from fastapi import FastAPI, HTTPException, Header
import os, requests, time

UP_URL   = os.environ["UPSTASH_URL"].rstrip("/")
UP_TOKEN = os.environ["UPSTASH_TOKEN"]
API_KEY  = os.getenv("API_KEY")  # 선택

app = FastAPI(title="KR Snapshot API (Free)", version="1.1")

def check_auth(x_api_key: str | None):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Unauthorized")

def upstash_hgetall(key: str) -> dict:
    headers = {"Authorization": f"Bearer {UP_TOKEN}"}

    # 1) 우선 /pipeline 방식 시도 (권장)
    try:
        r = requests.post(
            f"{UP_URL}/pipeline",
            headers=headers,
            json={"commands": [["HGETALL", key]]},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            # pipeline 응답은 보통 [{"result":[...]}] 형태
            arr = None
            if isinstance(data, list) and data:
                arr = data[0].get("result")
            elif isinstance(data, dict):
                arr = data.get("result")

            if isinstance(arr, list):
                it = iter(arr)
                return {k: v for k, v in zip(it, it)}
            # 예상 포맷이 아니면 아래 fallback으로
        else:
            # 400/404 등은 fallback으로
            pass
    except Exception:
        pass

    # 2) fallback: 루트 POST {"command": ["HGETALL", key]} 방식
    r = requests.post(
        f"{UP_URL}",
        headers=headers,
        json={"command": ["HGETALL", key]},
        timeout=10
    )
    r.raise_for_status()
    data = r.json()
    # 이 스타일은 보통 {"result":[...]} 형태
    arr = data.get("result") if isinstance(data, dict) else None
    if isinstance(arr, list):
        it = iter(arr)
        return {k: v for k, v in zip(it, it)}
    return {}

@app.get("/snapshot")
def snapshot(ticker: str, x_api_key: str | None = Header(default=None)):
    check_auth(x_api_key)
    d = upstash_hgetall(f"SNAP:{ticker}")
    if not d:
        raise HTTPException(404, f"No snapshot for {ticker} (maybe not fetched yet)")
    return {
        "ticker": ticker,
        "serverTs": int(time.time()*1000),
        "dataTs": int(d.get("ts", 0)),
        "price": float(d.get("price", 0)),
        "note": "polled every ~2 minutes (free plan)"
    }
