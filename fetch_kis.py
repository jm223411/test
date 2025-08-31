# fetch_kis.py  (최종본)
# - KIS 토큰 레이트리밋 자동 처리(EGW00133)
# - Upstash REST /pipeline(JSON 배열) 사용
# - SNAP:<티커> (스냅샷), SNAPSEQ:<티커> (시퀀스) 저장
# - TTL 기본 300초
import os, time, json, requests, sys
from typing import List, Dict

# ========= 환경 변수 확인 =========
REQUIRED = ["KIS_APP_KEY", "KIS_APP_SECRET", "UPSTASH_URL", "UPSTASH_TOKEN"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    print(f"[FATAL] Missing secrets: {missing}", file=sys.stderr)
    sys.exit(1)

APP_KEY    = os.environ["KIS_APP_KEY"]
APP_SECRET = os.environ["KIS_APP_SECRET"]
UP_URL     = os.environ["UPSTASH_URL"].rstrip("/")   # e.g. https://xxxx.upstash.io
UP_TOKEN   = os.environ["UPSTASH_TOKEN"]
TICKERS    = (os.getenv("TICKERS") or "005930,000660").split(",")
TTL_SEC    = int(os.getenv("TTL_SEC") or "300")      # 기본 300초
BASE       = os.getenv("KIS_BASE") or "https://openapi.koreainvestment.com:9443"

# ========= 공용 세션 =========
sess = requests.Session()
sess.headers.update({"accept": "application/json"})

# ========= KIS 토큰 발급 =========
def kis_get_access_token() -> str:
    url = f"{BASE}/oauth2/tokenP"  # 개인(P)
    payload = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    headers = {"content-type": "application/json; charset=utf-8", "accept": "application/json"}

    # 1차 시도
    r = sess.post(url, json=payload, headers=headers, timeout=20)
    if r.status_code == 200:
        j = r.json()
        tok = j.get("access_token")
        if not tok:
            raise RuntimeError(f"[KIS][token] no access_token: {j}")
        return tok

    # 레이트리밋(분당 1회) 처리
    text = r.text
    if r.status_code == 403 and ("EGW00133" in text or "1분당 1회" in text):
        print("[KIS][token] rate-limited (EGW00133). Sleep 65s and retry...", file=sys.stderr)
        time.sleep(65)
        r2 = sess.post(url, json=payload, headers=headers, timeout=20)
        r2.raise_for_status()
        j = r2.json()
        tok = j.get("access_token")
        if not tok:
            raise RuntimeError(f"[KIS][token] no access_token: {j}")
        return tok

    # 기타 5xx 백오프 한 번
    if 500 <= r.status_code < 600:
        print(f"[KIS][token] {r.status_code}. Sleep 5s retry...", file=sys.stderr)
        time.sleep(5)
        r3 = sess.post(url, json=payload, headers=headers, timeout=20)
        r3.raise_for_status()
        j = r3.json()
        tok = j.get("access_token")
        if not tok:
            raise RuntimeError(f"[KIS][token] no access_token: {j}")
        return tok

    # 실패
    print(f"[KIS][token] HTTP {r.status_code} {text[:300]}", file=sys.stderr)
    r.raise_for_status()
    raise RuntimeError("[KIS][token] unexpected")  # safety

# ========= KIS 현재가 조회 =========
def kis_get_price(token: str, ticker: str) -> float:
    url = f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHKST01010100",                # 현재가 조회 TR (실전/모의 참고: 문서에 따름)
        "custtype": "P",                         # 개인
        "content-type": "application/json; charset=utf-8",
        "accept": "application/json",
    }
    params = {
        "fid_cond_mrkt_div_code": "J",          # 주식
        "fid_input_iscd": ticker,               # 6자리 종목코드
    }
    r = sess.get(url, headers=headers, params=params, timeout=20)
    if r.status_code != 200:
        print(f"[KIS][price][{ticker}] HTTP {r.status_code} {r.text[:300]}", file=sys.stderr)
        r.raise_for_status()
    j = r.json()
    out = j.get("output") or {}
    if "stck_prpr" not in out:
        raise RuntimeError(f"[KIS][price][{ticker}] unexpected body: {j}")
    return float(out["stck_prpr"])

# ========= Upstash /pipeline 유틸 (JSON 배열 본문) =========
def upstash_pipeline(cmds: List[List[str]]):
    r = sess.post(
        f"{UP_URL}/pipeline",
        headers={
            "Authorization": f"Bearer {UP_TOKEN}",
            "Content-Type": "application/json",
        },
        json=cmds,          # ★ 배열 본문이어야 함
        timeout=20,
    )
    if r.status_code != 200:
        raise RuntimeError(f"[UPSTASH] HTTP {r.status_code} {r.text[:300]}")
    return r.json()

def save_snapshot_and_sequence(ticker: str, price: float, now_ms: int):
    key_snap = f"SNAP:{ticker}"
    key_seq  = f"SNAPSEQ:{ticker}"

    # HSET + EXPIRE (스냅샷), LPUSH/LTRIM (시퀀스)
    cmds = [
        ["HSET",   key_snap, "ts", str(now_ms), "price", str(price)],
        ["EXPIRE", key_snap, str(TTL_SEC)],
        ["LPUSH",  key_seq,  f"{now_ms}:{price}"],
        ["LTRIM",  key_seq,  "0", "599"]  # 최근 600개(약 10시간 @1분) 유지
    ]
    upstash_pipeline(cmds)

# ========= 메인 =========
def main():
    try:
        token = kis_get_access_token()
    except Exception as e:
        print(f"[FATAL] token error: {e}", file=sys.stderr)
        sys.exit(1)

    now_ms = int(time.time() * 1000)
    any_success = False

    for t in TICKERS:
        t = t.strip()
        if not t: continue
        try:
            price = kis_get_price(token, t)
            save_snapshot_and_sequence(t, price, now_ms)
            print(f"saved {t} {price}")
            any_success = True
            # (선택) API 레이트 고려해 미세 대기
            time.sleep(0.2)
        except Exception as e:
            print(f"err {t} {e}", file=sys.stderr)
            # 다음 티커 계속

    # 전체 실패라도 0으로 종료해 크론이 계속 돌도록 할 수도 있음
    # 관리상 실패 감지 원하면 아래 주석 해제
    # if not any_success:
    #     sys.exit(1)

if __name__ == "__main__":
    main()
