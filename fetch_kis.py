# fetch_kis.py — 최종본 (현재가 + 일봉 60개 자동 백필)
# - Upstash REST /pipeline(JSON 배열) 사용
# - SNAP:<티커> (스냅샷: ts, price) 저장 + TTL(기본 300초)
# - SNAPSEQ:<티커> (가격 시퀀스) 자동 관리
#   · 길이 < 60 이면: KIS “일봉 60개”를 한 번에 백필(주말/휴일에도 OK)
#   · 길이 ≥ 60 이면: 분당 1포인트만 추가
# - KIS 토큰 레이트리밋(EGW00133) 자동 대기 재시도
# - 일부 종목 실패해도 전체 파이프라인은 계속 진행

import os, time, json, requests, sys
from typing import List, Dict
from datetime import datetime, timezone, timedelta

# ========= 필수 시크릿 =========
REQUIRED = ["KIS_APP_KEY", "KIS_APP_SECRET", "UPSTASH_URL", "UPSTASH_TOKEN"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    print(f"[FATAL] Missing secrets: {missing}", file=sys.stderr)
    sys.exit(1)

# ========= 환경변수 =========
APP_KEY    = os.environ["KIS_APP_KEY"]
APP_SECRET = os.environ["KIS_APP_SECRET"]
UP_URL     = os.environ["UPSTASH_URL"].rstrip("/")   # e.g. https://xxxx.upstash.io
UP_TOKEN   = os.environ["UPSTASH_TOKEN"]
TICKERS    = (os.getenv("TICKERS") or "005930,000660,035420,035720,051910,068270,105560,000270,012330,055550,096770,066570,005380,000810,003550,034730,017670,015760,086790,251270").split(",")
TTL_SEC    = int(os.getenv("TTL_SEC") or "300")      # SNAP TTL (기본 300초)
BASE       = os.getenv("KIS_BASE") or "https://openapi.koreainvestment.com:9443"

# TR ID (계정/환경에 따라 다를 수 있어 오버라이드 가능)
TR_PRICE = os.getenv("KIS_TR_PRICE")  or "FHKST01010100"  # 현재가
TR_DAILY = os.getenv("KIS_TR_DAILY")  or "FHKST03010100"  # 일봉 (기간별 시세)

# 타임존 (일봉 날짜 파싱용)
KST = timezone(timedelta(hours=9))

# 공용 세션
sess = requests.Session()
sess.headers.update({"accept": "application/json"})


# ========= Upstash 유틸 (/pipeline: JSON 배열 본문) =========
def upstash_pipeline(cmds: List[List[str]]):
    r = sess.post(
        f"{UP_URL}/pipeline",
        headers={"Authorization": f"Bearer {UP_TOKEN}", "Content-Type": "application/json"},
        json=cmds,  # ★ 반드시 배열(JSON array)
        timeout=20,
    )
    if r.status_code != 200:
        raise RuntimeError(f"[UPSTASH] HTTP {r.status_code} {r.text[:300]}")
    return r.json()

def upstash_llen(key: str) -> int:
    try:
        data = upstash_pipeline([["LLEN", key]])
        res = data[0].get("result")
        return int(res) if res is not None else 0
    except Exception:
        return 0

def save_snapshot(ticker: str, price: float, now_ms: int):
    key_snap = f"SNAP:{ticker}"
    cmds = [
        ["HSET",   key_snap, "ts", str(now_ms), "price", str(price)],
        ["EXPIRE", key_snap, str(TTL_SEC)],
    ]
    upstash_pipeline(cmds)

def append_seq_point(ticker: str, price: float, ts_ms: int):
    key_seq = f"SNAPSEQ:{ticker}"
    cmds = [
        ["LPUSH", key_seq, f"{ts_ms}:{price}"],
        ["LTRIM", key_seq, "0", "599"],  # 최근 600개 유지
    ]
    upstash_pipeline(cmds)

def backfill_seq_with_daily(ticker: str, rows: List[Dict], max_keep: int = 600):
    """
    일봉 rows(오래된→최근 순)로 SNAPSEQ:<ticker>를 초기화/백필.
    rows 각 원소는 KIS 응답의 항목이며, 'stck_bsop_date'와 'stck_clpr' 사용.
    """
    key_seq = f"SNAPSEQ:{ticker}"
    cmds: List[List[str]] = [["DEL", key_seq]]
    # 오래된 → 최근 순으로 순회하면서 LPUSH 하면 최종적으로 '최근'이 앞(head)이 됨
    for r in rows:
        try:
            # 날짜(YYYYMMDD) → epoch ms (KST 00:00 기준)
            d = r.get("stck_bsop_date") or r.get("bas_dt")  # 계정/TR마다 키가 다를 수 있음
            p = r.get("stck_clpr") or r.get("close") or r.get("prc")
            if not d or p is None:
                continue
            dt = datetime.strptime(str(d), "%Y%m%d").replace(tzinfo=KST)
            ts_ms = int(dt.timestamp() * 1000)
            price = float(p)
        except Exception:
            continue
        cmds.append(["LPUSH", key_seq, f"{ts_ms}:{price}"])
    cmds.append(["LTRIM", key_seq, "0", str(max_keep - 1)])
    upstash_pipeline(cmds)


# ========= KIS 토큰/호출 =========
def kis_get_access_token() -> str:
    url = f"{BASE}/oauth2/tokenP"
    payload = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    headers = {"content-type": "application/json; charset=utf-8", "accept": "application/json"}

    r = sess.post(url, json=payload, headers=headers, timeout=20)
    if r.status_code == 200:
        tok = r.json().get("access_token")
        if not tok:
            raise RuntimeError("[KIS][token] no access_token")
        return tok

    # 레이트리밋(분당 1회) — EGW00133
    if r.status_code == 403 and ("EGW00133" in r.text or "1분당 1회" in r.text):
        print("[KIS][token] rate-limited. Sleep 65s & retry...", file=sys.stderr)
        time.sleep(65)
        r2 = sess.post(url, json=payload, headers=headers, timeout=20)
        r2.raise_for_status()
        tok = r2.json().get("access_token")
        if not tok:
            raise RuntimeError("[KIS][token] no access_token(after retry)")
        return tok

    # 5xx 한번 재시도
    if 500 <= r.status_code < 600:
        print(f"[KIS][token] {r.status_code}. Sleep 5s & retry...", file=sys.stderr)
        time.sleep(5)
        r3 = sess.post(url, json=payload, headers=headers, timeout=20)
        r3.raise_for_status()
        tok = r3.json().get("access_token")
        if not tok:
            raise RuntimeError("[KIS][token] no access_token(after 5xx retry)")
        return tok

    print(f"[KIS][token] HTTP {r.status_code} {r.text[:300]}", file=sys.stderr)
    r.raise_for_status()
    raise RuntimeError("[KIS][token] unexpected failure")

def kis_get_price(token: str, ticker: str) -> float:
    url = f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": TR_PRICE,
        "custtype": "P",
        "content-type": "application/json; charset=utf-8",
        "accept": "application/json",
    }
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": ticker}
    r = sess.get(url, headers=headers, params=params, timeout=20)
    if r.status_code != 200:
        print(f"[KIS][price][{ticker}] HTTP {r.status_code} {r.text[:300]}", file=sys.stderr)
        r.raise_for_status()
    out = r.json().get("output", {})
    if "stck_prpr" not in out:
        raise RuntimeError(f"[KIS][price][{ticker}] unexpected body: {r.text[:300]}")
    return float(out["stck_prpr"])

def kis_get_daily(token: str, ticker: str, count: int = 60) -> List[Dict]:
    url = f"{BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": TR_DAILY,        # 기본값: FHKST03010100 (환경에 맞게 유지)
        "custtype": "P",
        "content-type": "application/json; charset=utf-8",
        "accept": "application/json",
    }
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": ticker,
        "fid_period_div_code": "D",   # ★ 누락되어 있던 부분 (일/주/월/년 구분)
        "fid_org_adj_prc": "0",       # 필요 시 "1"
    }
    r = sess.get(url, headers=headers, params=params, timeout=20)
    if r.status_code != 200:
        print(f"[KIS][daily][{ticker}] HTTP {r.status_code} {r.text[:300]}", file=sys.stderr)
        r.raise_for_status()
    rows = r.json().get("output", []) or []

    # 오래된→최근 정렬 (날짜 키가 있다면)
    try:
        rows.sort(key=lambda d: d.get("stck_bsop_date") or d.get("bas_dt"))
    except Exception:
        pass

    return rows[-count:]



# ========= 메인 로직 =========
def main():
    try:
        token = kis_get_access_token()
    except Exception as e:
        print(f"[FATAL] token error: {e}", file=sys.stderr)
        sys.exit(1)

    now_ms = int(time.time() * 1000)
    any_success = False

    for raw in TICKERS:
        t = raw.strip()
        if not t:
            continue
        try:
            # 1) 현재가 스냅샷 저장
            price = kis_get_price(token, t)
            save_snapshot(t, price, now_ms)

            # 2) 시퀀스 길이 확인 → 부족 시 일봉 60개 백필, 충분하면 1포인트만 추가
            llen = upstash_llen(f"SNAPSEQ:{t}")
            if llen < 60:
                try:
                    rows = kis_get_daily(token, t, 60)
                    if rows:
                        backfill_seq_with_daily(t, rows, max_keep=600)
                        # 백필 후 “현재가” 포인트도 가장 최신으로 한 개 더 추가(선택)
                        append_seq_point(t, price, now_ms)
                        print(f"backfilled {t} daily60 + appended latest {price}")
                    else:
                        # 일봉이 비정상이라면 최신 포인트만 추가
                        append_seq_point(t, price, now_ms)
                        print(f"warn {t} daily empty; appended latest {price}")
                except Exception as be:
                    # 백필 실패해도 최신 포인트는 추가
                    append_seq_point(t, price, now_ms)
                    print(f"warn {t} backfill failed: {be}; appended latest {price}", file=sys.stderr)
            else:
                # 이미 충분 → 최신 포인트만 추가
                append_seq_point(t, price, now_ms)
                print(f"saved {t} {price}")

            any_success = True
            time.sleep(0.2)  # TR 부담 완화(필요시 조정)

        except Exception as e:
            print(f"err {t} {e}", file=sys.stderr)
            # 다음 티커 계속

    # 전체 실패 시 종료코드 1로 바꾸고 싶다면 아래 주석 해제
    # if not any_success:
    #     sys.exit(1)

if __name__ == "__main__":
    main()
