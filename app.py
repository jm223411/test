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
# === (app.py에 추가) 지표 계산 유틸 ================================
def ema(values, period):
    if len(values) < period: return []
    k = 2/(period+1)
    e = [sum(values[:period])/period]
    for v in values[period:]:
        e.append(e[-1] + k*(v - e[-1]))
    return e

def rsi(values, period=14):
    # 단순 RSI (Wilder 근사)
    if len(values) <= period: return None
    gains = []
    losses = []
    for i in range(1, period+1):
        ch = values[i]-values[i-1]
        gains.append(max(ch,0.0))
        losses.append(max(-ch,0.0))
    avg_gain = sum(gains)/period
    avg_loss = sum(losses)/period
    rsis = []
    for i in range(period+1, len(values)):
        ch = values[i]-values[i-1]
        gain = max(ch,0.0)
        loss = max(-ch,0.0)
        avg_gain = (avg_gain*(period-1)+gain)/period
        avg_loss = (avg_loss*(period-1)+loss)/period
        if avg_loss == 0: rs = float('inf')
        else: rs = avg_gain/avg_loss
        rsis.append(100 - (100/(1+rs)))
    return rsis[-1] if rsis else None

def macd(values, f=12, s=26, sig=9):
    if len(values) < s+sig: return None, None, None
    ema_f = ema(values, f)
    ema_s = ema(values, s)
    # 정렬 맞추기
    macd_line = [a-b for a,b in zip(ema_f[-len(ema_s):], ema_s)]
    if len(macd_line) < sig: return None, None, None
    signal = ema(macd_line, sig)
    hist = macd_line[-len(signal):][-1] - signal[-1]
    return macd_line[-1], signal[-1], hist

def highest(values, n):
    if len(values) < n: return None
    return max(values[-n:])

# === (app.py에 추가) Redis 시퀀스 로드 ===============================
def load_prices_from_snapseq(ticker: str, max_n=300):
    # 최근 max_n개 (신규→과거 순) 가져와서 시간순(과거→신규)로 뒤집기
    data = upstash_pipeline([["LRANGE", f"SNAPSEQ:{ticker}", "0", str(max_n-1)]])
    arr = data[0].get("result") or []
    if not arr: return []
    # "ts:price" → price(float)
    prices = []
    for s in arr:
        try:
            _, p = s.split(":")
            prices.append(float(p))
        except:
            continue
    return list(reversed(prices))  # 과거→최근

# === (app.py에 추가) 종목 리스트 (간단 예시) ======================
# 실제로는 .env나 Redis/파일에서 불러오세요.
UNIVERSE = os.getenv("UNIVERSE", "005930,000660,035420,035720,051910,068270,105560,000270,012330,055550").split(",")

# === (app.py에 추가) 점수화 로직 ==================================
def score_ticker(prices):
    # 최소 길이 확인
    if len(prices) < 60:  # 60포인트 미만은 스킵
        return None, {}

    # 지표 계산
    rsi14 = rsi(prices, 14)
    macd_line, macd_sig, macd_hist = macd(prices, 12, 26, 9)
    sma20 = sum(prices[-20:])/20 if len(prices)>=20 else None
    sma60 = sum(prices[-60:])/60 if len(prices)>=60 else None
    last = prices[-1]
    hi20 = highest(prices, 20)

    # 룰 기반 점수 (가중치 합산: 0~100 근사)
    score = 0
    reasons = []

    # 1) 상승추세: 가격 > SMA20 > SMA60
    if sma20 and sma60 and last > sma20 > sma60:
        score += 30
        reasons.append("추세 우상향(가격>SMA20>SMA60)")

    # 2) MACD 양호: MACD > 시그널
    if macd_line is not None and macd_sig is not None and macd_line > macd_sig:
        score += 25
        reasons.append("MACD 상방")

    # 3) RSI 중립~강세: 50~70
    if rsi14 is not None and 50 <= rsi14 <= 70:
        score += 20
        reasons.append(f"RSI {int(rsi14)} (중립~강세)")

    # 4) 20기간 고점 근접(돌파 시도)
    if hi20 and last >= 0.995*hi20:
        score += 15
        reasons.append("20기간 고점 근접/돌파 시도")

    # 5) 단기 모멘텀: 최근 5개 수익률 합 양수
    if len(prices) >= 6:
        mom = sum(prices[-i]-prices[-i-1] for i in range(1,6))
        if mom > 0:
            score += 10
            reasons.append("단기 모멘텀(+)")
    return score, {
        "last": last,
        "sma20": sma20,
        "sma60": sma60,
        "rsi14": rsi14,
        "macd": macd_line,
        "signal": macd_sig,
        "hi20": hi20,
        "notes": reasons
    }

# === (app.py에 추가) 추천 엔드포인트 ===============================
@app.get("/recommendations")
def recommendations(x_api_key: str | None = Header(default=None), n: int = 10):
    """
    기술적 분석 기반 상위 n개 종목 추천 (UNIVERSE 내)
    - 가격 시퀀스는 SNAPSEQ:<ticker> 사용 (1분 주기 수집 가정)
    """
    check_auth(x_api_key)
    results = []
    for t in UNIVERSE:
        prices = load_prices_from_snapseq(t, max_n=300)
        sc, info = score_ticker(prices)
        if sc is not None:
            results.append({"ticker": t, "score": sc, **info})

    if not results:
        raise HTTPException(503, "시퀀스 데이터가 부족합니다. 수집이 더 진행되면 다시 시도하세요.")

    # 점수로 내림차순 정렬 후 상위 n개
    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:max(1, n)]

    # 캐시 저장(선택): 유지 10분
    try:
        body = [
            ["SET", "RECO:TOP10", json.dumps(top)],
            ["EXPIRE", "RECO:TOP10", "600"]
        ]
        upstash_pipeline(body)
    except Exception:
        pass

    return {"generatedAt": int(time.time()*1000), "items": top}


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
