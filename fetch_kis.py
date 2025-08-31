# fetch_kis.py (디버그/안정화 버전)
import os, time, json, requests, sys

APP_KEY    = os.environ["KIS_APP_KEY"]
APP_SECRET = os.environ["KIS_APP_SECRET"]
UP_URL     = os.environ["UPSTASH_URL"].rstrip("/")
UP_TOKEN   = os.environ["UPSTASH_TOKEN"]
TICKERS    = (os.getenv("TICKERS") or "005930,000660").split(",")

# ※ 운영/모의 환경에 따라 BASE가 다를 수 있음
BASE = "https://openapi.koreainvestment.com:9443"

def get_access_token():
    url = f"{BASE}/oauth2/tokenP"  # 개인(P) 토큰
    payload = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code != 200:
            print(f"[KIS][token] HTTP {r.status_code} {r.text[:300]}", file=sys.stderr)
            r.raise_for_status()
        j = r.json()
        token = j.get("access_token")
        if not token:
            raise RuntimeError(f"[KIS][token] no access_token in response: {j}")
        return token
    except Exception as e:
        raise RuntimeError(f"[KIS][token] failed: {e}")

def get_price(token, ticker):
    # 국내주식 현재가 조회 (대표 tr_id 예시)
    url = f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHKST01010100",   # 현재가 조회용 TR (모의/실전 동일 케이스)
        "custtype": "P",            # 개인
        "content-type": "application/json; charset=utf-8",
        "accept": "application/json",
    }
    params = {
        "fid_cond_mrkt_div_code": "J",  # 주식
        "fid_input_iscd": ticker,       # 6자리 종목코드
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        if r.status_code != 200:
            print(f"[KIS][price][{ticker}] HTTP {r.status_code} {r.text[:300]}", file=sys.stderr)
            r.raise_for_status()
        j = r.json()
        # 응답 구조는 문서/계정 권한에 따라 다를 수 있음 → 대표 키들 확인
        out = j.get("output") or {}
        if "stck_prpr" not in out:
            raise RuntimeError(f"[KIS][price][{ticker}] unexpected body: {j}")
        return float(out["stck_prpr"])
    except Exception as e:
        raise RuntimeError(f"[KIS][price][{ticker}] failed: {e}")

def upstash_hset_pipeline(key, mapping: dict, ttl_sec: int = 500):
    # 값은 모두 문자열로!
    flat = []
    for k, v in mapping.items():
        flat.append(k)
        flat.append(str(v))
    body = [
        ["HSET", key] + flat,
        ["EXPIRE", key, str(ttl_sec)]
    ]
    r = requests.post(
        f"{UP_URL}/pipeline",
        headers={"Authorization": f"Bearer {UP_TOKEN}", "Content-Type": "application/json"},
        json=body,               # <<< 배열 본문
        timeout=15,
    )
    if r.status_code != 200:
        raise RuntimeError(f"[UPSTASH] HTTP {r.status_code} {r.text[:300]}")

def main():
    try:
        token = get_access_token()
        now_ms = int(time.time() * 1000)
        for t in TICKERS:
            try:
                price = get_price(token, t)
                key = f"SNAP:{t}"
                upstash_hset_pipeline(key, {"ts": now_ms, "price": price}, ttl_sec=180)
                print(f"saved {t} {price}")
            except Exception as ie:
                print(f"err {t} {ie}", file=sys.stderr)
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
