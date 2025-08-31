# app.py  v1.4 (snapshot + health + recommendations)
from fastapi import FastAPI, HTTPException, Header
import os, requests, time, json, statistics

# ── 환경변수 ─────────────────────────────────────────────────────────
UP_URL   = os.environ["UPSTASH_URL"].rstrip("/")
UP_TOKEN = os.environ["UPSTASH_TOKEN"]
API_KEY  = os.getenv("API_KEY")  # 선택
# 추천 대상 종목 목록(쉼표 구분). Render 환경변수에서 관리: "005930,000660,035420,035720,051910,068270,105560,000270,012330,055550"
UNIVERSE = os.getenv("UNIVERSE", "005930,000660,035420,035720,051910,068270,105560,000270,012330,055550,096770,066570,005380,000810,003550,034730,017670,015760,086790,251270,207940
").split(",")

app = FastAPI(title="KR Snapshot API (with Recommendations)", version="1.4")

# ── 공통 ─────────────────────────────────────────────────────────────
def check_auth(x_api_key: str | None):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Unauthorized")

def upstash_pipeline(cmds: list[list[str]]):
    r = requests.post(
        f"{UP_URL}/pipeline",
        headers={"Authorization": f"Bearer {UP_TOKEN}", "Content-Type": "application/json"},
        json=cmds,  # 반드시 JSON 배열
        timeout=10,
    )
    if r.status_code != 200:
        raise HTTPException(502, f"Upstash {r.status_code}: {r.text}")
    return r.json()

def upstash_hgetall(key: str) -> dict:
    # 1) pipeline
    try:
        data = upstash_pipeline([["HGETALL", key]])
        arr = None
        if isinstance(data, list) and data:
            arr = data[0].get("result")
        elif isinstance(data, dict):
            arr = data.get("result")
        if isinstance(arr, list):
            it = iter(arr)
            return {k: v for k, v in zip(it, it)}
    except Exception:
        pass
    # 2) path
    try:
        r = requests.get(f"{UP_URL}/HGETALL/{key}", headers={"Authorization": f"Bearer {UP_TOKEN}"}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            arr = data.get("result") if isinstance(data, dict) else None
            if isinstance(arr, list):
                it = iter(arr)
                return {k: v for k, v in zip(it, it)}
    except Exception:
        pass
    # 3) command
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
    return {}

def load_prices_from_snapseq(ticker: str, max_n=300):
    # 최근 max_n개(신규→과거) → 과거→최근 순으로 정렬
    data = upstash_pipeline([["LRANGE", f"SNAPSEQ:{ticker}", "0", str(max_n-1)]])
    arr = data[0].get("result") or []
    if not arr: return []
    prices = []
    for s in arr:
        try:
            _, p = s.split(":")
            prices.append(float(p))
        except:
            continue
    return list(reversed(prices))

# ── 지표 계산(간단) ─────────────────────────────────────────────────
def ema(values, period):
    if len(values) < period: return []
    k = 2/(period+1)
    e = [sum(values[:period])/period]
    for v in values[period:]:
        e.append(e[-1] + k*(v - e[-1]))
    return e

def rsi(values, period=14):
    if len(values) <= period: return None
    gains, losses = [], []
    for i in range(1, period+1):
        ch = values[i]-values[i-1]
        gains.append(max(ch,0.0)); losses.append(max(-ch,0.0))
    avg_gain = sum(gains)/period; avg_loss = sum(losses)/period
    rsis = []
    for i in range(period+1, len(values)):
        ch = values[i]-values[i-1]
        gain = max(ch,0.0); loss = max(-ch,0.0)
        avg_gain = (avg_gain*(period-1)+gain)/period
        avg_loss = (avg_loss*(period-1)+loss)/period
        rs = float('inf') if avg_loss==0 else (avg_gain/avg_loss)
        rsis.append(100 - (100/(1+rs)))
    return rsis[-1] if rsis else None

def macd(values, f=12, s=26, sig=9):
    if len(values) < s+sig: return None, None, None
    ema_f = ema(values, f); ema_s = ema(values, s)
    macd_line = [a-b for a,b in zip(ema_f[-len(ema_s):], ema_s)]
    if len(macd_line) < sig: return None, None, None
    signal = ema(macd_line, sig)
    hist = macd_line[-len(signal):][-1] - signal[-1]
    return macd_line[-1], signal[-1], hist

def highest(values, n):
    if len(values) < n: return None
    return max(values[-n:])

def score_ticker(prices):
    # 최소 길이
    if len(prices) < 60:
        return None, {}
    last = prices[-1]
    sma20 = sum(prices[-20:])/20 if len(prices)>=20 else None
    sma60 = sum(prices[-60:])/60 if len(prices)>=60 else None
    rsi14 = rsi(prices, 14)
    macd_line, macd_sig, macd_hist = macd(prices, 12, 26, 9)
    hi20 = highest(prices, 20)

    score = 0; reasons = []
    # 추세: 가격 > SMA20 > SMA60
    if sma20 and sma60 and last > sma20 > sma60:
        score += 30; reasons.append("추세 우상향(가격>SMA20>SMA60)")
    # MACD 상방
    if macd_line is not None and macd_sig is not None and macd_line > macd_sig:
        score += 25; reasons.append("MACD 상방")
    # RSI 50~70
    if rsi14 is not None and 50 <= rsi14 <= 70:
        score += 20; reasons.append(f"RSI {int(rsi14)} (중립~강세)")
    # 20기간 고점 근접/돌파
    if hi20 and last >= 0.995*hi20:
        score += 15; reasons.append("20기간 고점 근접/돌파 시도")
    # 단기 모멘텀(+)
    if len(prices) >= 6:
        mom = sum(prices[-i]-prices[-i-1] for i in range(1,6))
        if mom > 0:
            score += 10; reasons.append("단기 모멘텀(+)")
    return score, {
        "last": last, "sma20": sma20, "sma60": sma60,
        "rsi14": rsi14, "macd": macd_line, "signal": macd_sig,
        "hi20": hi20, "notes": reasons
    }

# ── 라우트 ──────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"ok": True, "message": "Use /snapshot?ticker=005930 or /recommendations?n=10", "version": "1.4"}

@app.get("/health")
def health():
    try:
        resp = upstash_pipeline([["PING"]])
        return {"ok": True, "method": "pipeline", "resp": resp}
    except Exception as e:
        raise HTTPException(502, f"Upstash REST failed: {e}")

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
        "note": "polled every ~1-2 minutes (free plan)"
    }

@app.get("/recommendations")
def recommendations(x_api_key: str | None = Header(default=None), n: int = 10):
    """
    기술적 분석 기반 상위 n개 추천 (UNIVERSE 범위 내)
    - SNAPSEQ:<ticker> 시퀀스를 사용(1분 주기 수집 가정)
    """
    check_auth(x_api_key)
    results = []
    for t in UNIVERSE:
        t = t.strip()
        if not t: continue
        try:
            prices = load_prices_from_snapseq(t, max_n=300)
            sc, info = score_ticker(prices)
            if sc is not None:
                results.append({"ticker": t, "score": sc, **info})
        except Exception:
            continue

    if not results:
        raise HTTPException(503, "시퀀스 데이터가 부족합니다. 수집이 더 진행되면 다시 시도하세요.")

    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:max(1, n)]

    # (선택) 캐시
    try:
        upstash_pipeline([["SET", "RECO:TOP10", json.dumps(top)], ["EXPIRE", "RECO:TOP10", "600"]])
    except Exception:
        pass

    return {"generatedAt": int(time.time()*1000), "items": top}
