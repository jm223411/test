from fastapi import FastAPI, HTTPException, Header
import os, requests, time

UP_URL   = os.environ["UPSTASH_URL"]
UP_TOKEN = os.environ["UPSTASH_TOKEN"]
API_KEY  = os.getenv("API_KEY")  # 선택: 있으면 보안 강화

app = FastAPI(title="KR Snapshot API (Free)", version="1.0")

def check_auth(x_api_key: str | None):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Unauthorized")

def upstash_hgetall(key: str) -> dict:
    r = requests.post(UP_URL, headers={"Authorization": f"Bearer {UP_TOKEN}"},
                      json={"command": ["HGETALL", key]}, timeout=10)
    r.raise_for_status()
    arr = r.json()
    if not isinstance(arr, list): return {}
    it = iter(arr)
    return {k: v for k, v in zip(it, it)}

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
