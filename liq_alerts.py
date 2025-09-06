# liq_alerts_v5.py
import time, random, hashlib, requests, re
from datetime import datetime, timedelta, timezone
from collections import deque

# ====== TELEGRAM + COINALYZE (your details) ======
CHAT_IDS = ["-4869615280"]
COINALYZE_KEY = "30d603dd-9814-421e-94cc-62f9775c541c"
TELEGRAM_TOKEN = "8422686073:AAGmMzABWh9r8cyXrdpoWYldThb51AaK0Aw"
# ==================================================

# -------- Settings --------
LOWER_CAP = 50_000_000
UPPER_CAP = 500_000_000
MIN_24H_VOL = 10_000_000
RATIO_THRESHOLD = 0.0002   # 0.02%
MIN_LIQ_USD = 0

# Coinalyze pacing / limits
COINALYZE_CHUNK = 8          # reduce to 5 if 429s persist
PACE_SECONDS = 2.4
COINALYZE_GLOBAL_RPM = 30

COINGECKO_PACE_SECONDS = 1.8
MAX_RETRIES = 7
INITIAL_COOLDOWN = 1.0
SEND_NO_HITS_SUMMARY = True

# Manual overrides still supported
OVERRIDES = {}

# Explicit mapping to avoid symbol collisions (base → CG ID)
BASE_TO_CGID = {
    "btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "ripple",
    "bnb": "binancecoin", "ada": "cardano", "doge": "dogecoin",
    "ton": "the-open-network", "trx": "tron", "dot": "polkadot",
    "link": "chainlink", "avax": "avalanche-2", "matic": "polygon",
    "atom": "cosmos", "uni": "uniswap", "ltc": "litecoin", "xmr": "monero",
    "etc": "ethereum-classic", "near": "near", "algo": "algorand",
    "op": "optimism", "arb": "arbitrum", "apt": "aptos", "inj": "injective",
    "ftm": "fantom", "sui": "sui", "sei": "sei-network",
}

# Stables to exclude (WUSDT intentionally NOT included)
STABLE_BASES = {
    "usdt","usdc","usd","susd","gusd","tusd","dai","usde","usdp","usdd","usds","usdx"
}

# -------- Endpoints --------
COINALYZE_BASE = "https://api.coinalyze.net/v1"
TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
CG_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
CG_RANGE   = "https://api.coingecko.com/api/v3/coins/{id}/market_chart/range"

# -------- HTTP helpers --------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "liq-alerts/v5"})

def _sleep_jitter(base: float):
    time.sleep(base + random.random() * 0.25)

# Global soft rate limiter for Coinalyze
_call_times = deque()
def _coinalyze_rate_gate():
    now = time.time(); window = 60.0
    while _call_times and (now - _call_times[0] > window):
        _call_times.popleft()
    if len(_call_times) >= COINALYZE_GLOBAL_RPM:
        time.sleep(window - (now - _call_times[0]) + 0.05)
    _call_times.append(time.time())

def http_get_with_backoff(url, params=None, timeout=45):
    wait = 0.0; last_err = None; attempt = 1
    while attempt <= MAX_RETRIES:
        if wait > 0: _sleep_jitter(wait); wait = 0.0
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                try:
                    block = max(0.0, float(ra)) if ra is not None else 30.0
                except:
                    block = 30.0
                print(f"[WARN] 429 on {url}; blocking {block:.3f}s")
                time.sleep(block + 0.25)
                continue  # don’t count as an attempt
            if 500 <= r.status_code < 600:
                wait = min(2 * attempt, 30)
                print(f"[WARN] {r.status_code} on {url} attempt {attempt}/{MAX_RETRIES}; retry in {wait}s")
                attempt += 1; continue
            if not (200 <= r.status_code < 300):
                body = (r.text or "")[:300]
                raise RuntimeError(f"HTTP {r.status_code} {url} body={body}")
            return r
        except requests.RequestException as e:
            last_err = e; wait = min(2 * attempt, 30)
            print(f"[WARN] RequestException {e} on {url} attempt {attempt}/{MAX_RETRIES}; retry in {wait}s")
            attempt += 1
    raise RuntimeError(f"GET failed after retries: {url} last_err={last_err}")

def coinalyze_get(path, params=None, timeout=45):
    params = dict(params or {}); params["api_key"] = COINALYZE_KEY
    url = f"{COINALYZE_BASE}{path}"
    return http_get_with_backoff(url, params=params, timeout=timeout).json()

# -------- Common helpers --------
def send_tg(text: str):
    for cid in CHAT_IDS:
        try:
            SESSION.post(f"{TG_BASE}/sendMessage",
                         json={"chat_id": cid, "text": text, "disable_web_page_preview": True},
                         timeout=20)
        except Exception as e:
            print(f"[WARN] Telegram send failed: {e}")

def fmt_usd(x):
    try: return f"${x:,.0f}"
    except: return f"${x}"

def last_completed_hour_window():
    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    return int(start.timestamp()), int(end.timestamp())

def idem_key(*parts) -> str:
    m = hashlib.sha256()
    for p in parts: m.update(str(p).encode())
    return m.hexdigest()[:16]

def norm(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())

# -------- CoinGecko --------
def get_coins_in_cap_band_sorted():
    time.sleep(INITIAL_COOLDOWN)
    by_symbol = {}; page = 1
    while True:
        params = {"vs_currency":"usd","order":"market_cap_desc","per_page":250,
                  "page":page,"price_change_percentage":"1h,24h"}
        r = http_get_with_backoff(CG_MARKETS, params=params); batch = r.json()
        if not batch: break
        stop = False
        for c in batch:
            mc, vol = c.get("market_cap"), c.get("total_volume")
            if mc is None or vol is None: continue
            if mc < LOWER_CAP: stop = True; break
            if LOWER_CAP<=mc<=UPPER_CAP and vol>=MIN_24H_VOL:
                sym=(c.get("symbol") or "").lower()
                one_h=c.get("price_change_percentage_1h_in_currency")
                day=c.get("price_change_percentage_24h_in_currency")
                move=one_h if one_h is not None else day
                move_score=abs(move) if move is not None else 0.0
                row={"id":c.get("id"),"symbol":sym,"name":c.get("name"),
                     "market_cap":mc,"move_score":move_score,"total_volume":vol}
                if sym not in by_symbol or mc>by_symbol[sym]["market_cap"]:
                    by_symbol[sym]=row
        if stop: break
        page+=1
        if page>20: break
        _sleep_jitter(COINGECKO_PACE_SECONDS)
    coins=list(by_symbol.values())
    coins.sort(key=lambda x:x["move_score"],reverse=True)
    return coins

def get_market_cap_at_close(coin_id, ts_end):
    frm=ts_end-30*60; to=ts_end+1
    url=CG_RANGE.format(id=coin_id)
    r=http_get_with_backoff(url,params={"vs_currency":"usd","from":frm,"to":to})
    series=r.json().get("market_caps") or []
    if not series: return 0.0
    target_ms=ts_end*1000; best=None
    for ms,val in series:
        if ms<=target_ms: best=val
        else: break
    if best is None: best=series[-1][1]
    return float(best or 0.0)

# -------- Coinalyze --------
_QUOTE_SUFFIXES = ["USDT","USD","USDC","BUSD","FDUSD","USDE","TUSD","USDP","USDD","USDX"]

def _base_from_symbol(sym: str) -> str:
    head = sym.split("_PERP", 1)[0]  # e.g. SOMIUSDT
    for q in _QUOTE_SUFFIXES:
        if head.endswith(q):
            return head[:-len(q)]
    return head

def group_perps_by_base():
    """
    Prefer aggregated .A markets; if no .A, fall back to ALL per-exchange.
    Index groups under BOTH normalized base_asset and normalized base parsed from symbol.
    Returns: (groups: dict[str, list[str]], markets: list[dict])
    """
    markets = coinalyze_get("/future-markets", {})
    agg = {}
    per_ex = {}
    for m in markets:
        sym = m.get("symbol","")
        if "_PERP" not in sym: continue
        b1 = norm(m.get("base_asset",""))     # e.g. "somnia"
        b2 = norm(_base_from_symbol(sym))     # e.g. "somi"
        target = agg if sym.endswith(".A") else per_ex
        if b1: target.setdefault(b1, []).append(sym)
        if b2 and b2 != b1: target.setdefault(b2, []).append(sym)
    groups = {}
    for k in set(agg) | set(per_ex):
        groups[k] = agg.get(k) or per_ex.get(k, [])
    return groups, markets

def resolve_syms_for_coin(coin, groups, markets):
    """
    Try multiple normalized keys (symbol/name/id). If none found, RESCUE by
    scanning raw market symbols like ^BASE(USDT|USD|USDC|...).*_PERP .
    """
    base_raw = (coin.get("symbol") or "")
    base_norm = norm(base_raw)
    candidates = [base_norm, norm(coin.get("name","")), norm(coin.get("id",""))]

    # 1) Normal path: keyed groups
    for k in candidates:
        if k and k in groups and groups[k]:
            return groups[k], "groups"

    # 2) Rescue: scan the symbols text
    if base_raw:
        base_up = base_raw.upper()
        quote_alt = "|".join(_QUOTE_SUFFIXES)
        rx = re.compile(rf"^{re.escape(base_up)}(?:{quote_alt}).*_PERP", re.IGNORECASE)
        hits = [m.get("symbol") for m in markets if rx.match(m.get("symbol",""))]
        if hits:
            print(f"[INFO] RESCUE matched {base_raw.upper()} via symbol scan: {hits[:4]}{'...' if len(hits)>4 else ''}")
            return hits, "rescue"

    return None, "none"

def liq_last_hour_by_base(base_to_symbols):
    frm,to=last_completed_hour_window()
    all_syms=sorted({s for syms in base_to_symbols.values() for s in syms})
    symbol_to_base={s:b for b,syms in base_to_symbols.items() for s in syms}
    totals={b:0.0 for b in base_to_symbols}
    raw_by_base={}
    if not all_syms:
        print("[INFO] No symbols to query in Coinalyze.")
        return totals,raw_by_base,frm,to
    CHUNK=COINALYZE_CHUNK
    for i in range(0,len(all_syms),CHUNK):
        chunk=all_syms[i:i+CHUNK]
        try:
            _coinalyze_rate_gate()
            data=coinalyze_get("/liquidation-history",{
                "symbols":",".join(chunk),"interval":"1hour",
                "from":frm,"to":to,"convert_to_usd":"true"})
        except Exception as e:
            print(f"[ERROR] Coinalyze batch failed (size={len(chunk)}): {e}")
            if len(chunk)>1:
                mid=len(chunk)//2
                for sub in (chunk[:mid],chunk[mid:]):
                    try:
                        _coinalyze_rate_gate()
                        data_sub=coinalyze_get("/liquidation-history",{
                            "symbols":",".join(sub),"interval":"1hour",
                            "from":frm,"to":to,"convert_to_usd":"true"})
                        _accumulate_liqs(data_sub,symbol_to_base,raw_by_base,totals,frm,to)
                    except Exception as e2:
                        print(f"[ERROR] Sub-chunk failed (size={len(sub)}): {e2}")
            continue
        _accumulate_liqs(data,symbol_to_base,raw_by_base,totals,frm,to)
        _sleep_jitter(PACE_SECONDS)
    return totals,raw_by_base,frm,to

def _accumulate_liqs(data,symbol_to_base,raw_by_base,totals,frm,to):
    for entry in data:
        b=symbol_to_base.get(entry.get("symbol"))
        if not b: continue
        hist=entry.get("history",[])
        if hist:
            raw_by_base.setdefault(b,[]).extend(hist)
            for c in reversed(hist):
                try: t=int(c.get("t",0))
                except: continue
                if frm<=t<=to:
                    l=float(c.get("l",0)); s=float(c.get("s",0))
                    totals[b]+=l+s; break

# -------- Main --------
_seen_alerts=set()

def run_once():
    coins=get_coins_in_cap_band_sorted()
    groups, markets = group_perps_by_base()

    base_to_symbols={}; unmatched=[]
    for coin in coins:
        base_raw=(coin["symbol"] or "")
        # Skip only true stables; WUSDT is allowed
        if base_raw.lower() in STABLE_BASES:
            continue

        # Overrides (optional)
        if base_raw.lower() in OVERRIDES:
            syms=OVERRIDES[base_raw.lower()]
            if isinstance(syms,str): syms=[syms]
            base_to_symbols[base_raw.lower()]=syms
            continue

        syms, how = resolve_syms_for_coin(coin, groups, markets)
        if syms:
            base_to_symbols[base_raw.lower()] = syms
        else:
            unmatched.append(f"{base_raw.upper()} ({coin['name']}) — no perps")

    liq_by_base,raw_by_base,frm,to=liq_last_hour_by_base(base_to_symbols)
    checked=0; alerted=0

    print(f"[INFO] Window {frm}->{to} ({datetime.utcfromtimestamp(frm)} – {datetime.utcfromtimestamp(to)} UTC)")

    for coin in coins:
        base_raw=(coin["symbol"] or "")
        if base_raw.lower() in STABLE_BASES: continue
        syms=base_to_symbols.get(base_raw.lower())
        if not syms: continue

        liq_usd=float(liq_by_base.get(base_raw.lower(),0.0))
        if liq_usd<MIN_LIQ_USD: continue
        checked+=1

        # pre-filter to avoid most CG /range calls
        est_ratio=liq_usd/max(coin["market_cap"],1)
        if est_ratio<(RATIO_THRESHOLD*0.6):
            print(f"[DEBUG] {base_raw.upper()} skipped by prefilter | liq={liq_usd:.2f} current_mc={coin['market_cap']:.2f} est_ratio={est_ratio:.5%}")
            continue

        cg_id=BASE_TO_CGID.get(base_raw.lower(),coin["id"])
        mc_close=get_market_cap_at_close(cg_id,to)
        if mc_close<=0 or not(LOWER_CAP<=mc_close<=UPPER_CAP):
            print(f"[DEBUG] {base_raw.upper()} dropped by historical MC band | mc_close={mc_close:.2f}")
            continue

        ratio=liq_usd/mc_close
        print(f"[DEBUG] {base_raw.upper()} | liq_usd={liq_usd:.2f} | mc_close={mc_close:.2f} | ratio={ratio:.5%} | syms={syms}")
        raw=raw_by_base.get(base_raw.lower(), [])[-3:]
        for c in raw:
            try: t=datetime.utcfromtimestamp(int(c['t']))
            except: continue
            l=float(c.get('l',0)); s=float(c.get('s',0))
            print(f"    [RAW] {base_raw.upper()} {t} UTC | long={l} short={s}")

        if ratio>=RATIO_THRESHOLD:
            key=idem_key(sorted(syms),frm,to,round(ratio,6))
            if key in _seen_alerts: continue
            _seen_alerts.add(key)
            window=(f"{datetime.fromtimestamp(frm,tz=timezone.utc):%Y-%m-%d %H:%M}"
                    f"–{datetime.fromtimestamp(to,tz=timezone.utc):%H:%M} UTC")
            msg=(f"🔔 Liq/MC ≥ {RATIO_THRESHOLD*100:.3f}% (at close)\n"
                 f"Ticker: {', '.join(syms)}\n"
                 f"Window: {window}\n"
                 f"Liquidations: {fmt_usd(liq_usd)}\n"
                 f"MC (close): {fmt_usd(mc_close)}\n"
                 f"Liq/MC: {ratio*100:.3f}%")
            send_tg(msg); alerted+=1

    if unmatched:
        print(f"[INFO] Unmatched bases: {len(unmatched)}")
        send_tg("⚠️ No perps:\n"+"\n".join(unmatched[:25]))
    if SEND_NO_HITS_SUMMARY and alerted==0:
        send_tg(f"ℹ️ Scan done: checked {checked}; no Liq/MC ≥ {RATIO_THRESHOLD*100:.3f}%")

if __name__=="__main__":
    try:
        run_once()
    except Exception as e:
        msg=f"❗ liq-alerts crashed: {e.__class__.__name__}: {e}"
        print(msg); send_tg(msg); raise
