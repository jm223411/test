import os, time, json, requests

APP_KEY    = os.environ["KIS_APP_KEY"]
APP_SECRET = os.environ["KIS_APP_SECRET"]
UP_URL     = os.environ["UPSTASH_URL"]
UP_TOKEN   = os.environ["UPSTASH_TOKEN"]
TICKERS    = os.getenv("TICKERS", "005930,000660").split(",")  # 삼성전자, 하이닉스

BASE = "https://openapi.koreainvestment.com:9443"

def get_access_token():
    url = f"{BASE}/oauth2/tokenP"
    payload = {"grant_type":"client_credentials","appkey":APP_KEY,"appsecret":APP_SECRET}
    r = requests.post(url, json=payload, timeout=10); r.raise_for_status()
    return r.json()["access_token"]

def get_price(token, ticker):
    url = f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY, "appsecret": APP_SECRET, "tr_id": "FHKST01010100"
    }
    params = {"fid_cond_mrkt_div_code":"J","fid_input_iscd":ticker}
    r = requests.get(url, headers=headers, params=params, timeout=10); r.raise_for_status()
    j = r.json()
    return float(j["output"]["stck_prpr"])  # 현재가(문서 기준 필드)

def upstash_hset(key, mapping: dict):
    cmd = ["HSET", key]
    for k, v in mapping.items(): cmd += [k, str(v)]
    r = requests.post(UP_URL, headers={"Authorization": f"Bearer {UP_TOKEN}"},
                      json={"command": cmd}, timeout=10); r.raise_for_status()

def upstash_expire(key, ttl_sec: int):
    r = requests.post(UP_URL, headers={"Authorization": f"Bearer {UP_TOKEN}"},
                      json={"command": ["EXPIRE", key, str(ttl_sec)]}, timeout=10)
    r.raise_for_status()

def main():
    token = get_access_token()
    now_ms = int(time.time()*1000)
    for t in TICKERS:
        try:
            p = get_price(token, t)
            key = f"SNAP:{t}"
            upstash_hset(key, {"ts": now_ms, "price": p})
            upstash_expire(key, 180)   # 3분 TTL
            print(f"saved {t} {p}")
        except Exception as e:
            print("err", t, e)

if __name__ == "__main__":
    main()
