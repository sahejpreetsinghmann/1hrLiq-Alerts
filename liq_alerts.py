# liq_alerts_v12_quoteaware_diag.py
import time, random, hashlib, requests, re
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict

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

# Coinalyze pacing / limits (conservative to avoid 429s)
COINALYZE_CHUNK = 4
PACE_SECONDS = 4.5
COINALYZE_GLOBAL_RPM = 15

# CoinGecko pacing (gentle)
COINGECKO_PACE_SECONDS = 2.6
MAX_RETRIES = 7
INITIAL_COOLDOWN = 1.2
SEND_NO_HITS_SUMMARY = True

# If liq < this, an alert is impossible even at LOWER_CAP
MIN_LIQ_FOR_POSSIBLE_ALERT = RATIO_THRESHOLD * LOWER_CAP  # e.g., $10,000

# Prefer aggregated markets when available (within each quote)
PREFER_AGGREGATED = True

# STRICT candle alignment: require t==frm?
EXACT_BAR_REQUIRED = False  # set True if you want strict exact-bar only

# --- Optional: UI emulation filters for log comparison ---
UI_EMU_QUOTES = None       # e.g., ["USDT"] or None for ALL
UI_EMU_EXCHANGES = None    # e.g., ["BYBIT","BINANCE"] or None for ALL
# ----------------------------------------------------------

# Manual overrides
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
STABLE_BASES = {"usdt","usdc","usd","susd","gusd","tusd","dai","usde","usdp","usdd","usds","usdx"}

# -------- Endpoints --------
COINALYZE_BASE = "https://api.coinalyze.net/v1"
TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
CG_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
CG_RANGE   = "https://api.coingecko.com/api/v3/coins/{id}/market_chart/range"

# -------- HTTP helpers --------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "liq-alerts/v12-quoteaware"})

def _sleep_jitter(base: float):
    time.sleep(base + random.random() * 0.35)

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
                    block = max(0.0, float(ra)) if ra is not None else 70.0
                except:
                    block = 70.0
                print(f"[WARN] 429 on {url}; blocking {block:.3f}s")
                time.sleep(block + 0.35)
                continue  # don’t count as an attempt
            if 500 <= r.status_code < 600:
                wait = min(2 * attempt, 35)
                print(f"[WARN] {r.status_code} on {url} attempt {attempt}/{MAX_RETRIES}; retry in {wait}s")
                attempt += 1; continue
            if not (200 <= r.status_code < 300):
                body = (r.text or "")[:300]
                raise RuntimeError(f"HTTP {r.status_code} {url} body={body}")
            return r
        except requests.RequestException as e:
            last_err = e; wait = min(2 * attempt, 35)
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
                         timeout=25)
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
    print(f"[INFO] CG candidates: {len(coins)}")
    return coins

# Cache for CG /range calls
_MC_CACHE = {}
_MC_CACHE_MAX = 240

def get_market_cap_at_close(coin_id, ts_end):
    key = (coin_id, ts_end)
    if key in _MC_CACHE:
        return _MC_CACHE[key]
    frm=ts_end-30*60; to=ts_end+1
    url=CG_RANGE.format(id=coin_id)
    r=http_get_with_backoff(url,params={"vs_currency":"usd","from":frm,"to":to})
    _sleep_jitter(2.2)
    series=r.json().get("market_caps") or []
    if not series:
        val = 0.0
    else:
        target_ms=ts_end*1000; best=None
        for ms,val_ in series:
            if ms<=target_ms: best=val_
            else: break
        if best is None: best=series[-1][1]
        val = float(best or 0.0)
    if len(_MC_CACHE) >= _MC_CACHE_MAX:
        _MC_CACHE.pop(next(iter(_MC_CACHE)))
    _MC_CACHE[key] = val
    return val

# -------- Coinalyze parsing helpers --------
_QUOTE_SUFFIXES = ["USDT","USD","USDC","BUSD","FDUSD","USDE","TUSD","USDP","USDD","USDX"]

def _base_from_symbol(sym: str) -> str:
    head = sym.split("_PERP", 1)[0]
    for q in _QUOTE_SUFFIXES:
        if head.endswith(q):
            return head[:-len(q)]
    return head

def _quote_from_symbol(sym: str) -> str:
    head = sym.split("_PERP", 1)[0]
    for q in _QUOTE_SUFFIXES:
        if head.endswith(q):
            return q
    return "UNKNOWN"

def _exchange_from_symbol(sym: str) -> str:
    seg = sym.split(".")[-1] if "." in sym else ""
    return seg or "UNKNOWN"

# -------- Coinalyze symbol maps (quote-aware) --------
def group_perps_by_base_quote_both():
    """
    Build TWO nested maps keyed by normalized base and quote:
      - agg[base][quote]   -> list of aggregated '.A' symbols for that base/quote
      - perex[base][quote] -> list of per-exchange symbols for that base/quote
    Keys include BOTH norm(base_asset) and norm(parsed symbol base), merged.
    """
    markets = coinalyze_get("/future-markets", {})
    agg = defaultdict(lambda: defaultdict(list))
    perex = defaultdict(lambda: defaultdict(list))
    for m in markets:
        sym = m.get("symbol","")
        if "_PERP" not in sym: continue
        q = _quote_from_symbol(sym)
        b1 = norm(m.get("base_asset",""))
        b2 = norm(_base_from_symbol(sym))
        target = agg if sym.endswith(".A") else perex
        if b1: target[b1][q].append(sym)
        if b2 and b2 != b1: target[b2][q].append(sym)
    # dedupe lists
    for base_map in (agg, perex):
        for b in list(base_map.keys()):
            for q in list(base_map[b].keys()):
                base_map[b][q] = sorted(set(base_map[b][q]))
    return agg, perex, markets

def resolve_syms_for_coin_quoteaware(coin, agg, perex, markets):
    """
    For each quote independently:
      - if aggregated '.A' exists under ANY candidate key -> use ONLY that aggregated symbol(s) for the quote
      - else if per-exchange exists                          -> use ALL per-exchange symbols for the quote
    Finally union across quotes. Never mix agg+perex for the same quote.
    """
    base_raw = (coin.get("symbol") or "")
    keys = [norm(base_raw), norm(coin.get("name","")), norm(coin.get("id",""))]

    # Collect possible symbols per quote across all candidate keys
    agg_by_q = defaultdict(list)
    perex_by_q = defaultdict(list)
    for k in keys:
        if not k: continue
        for q,lst in (agg.get(k) or {}).items():
            agg_by_q[q].extend(lst)
        for q,lst in (perex.get(k) or {}).items():
            perex_by_q[q].extend(lst)

    chosen = []
    chosen_mode = {}  # quote -> 'agg' or 'perex'
    # choose per quote
    for q in sorted(set(list(agg_by_q.keys()) + list(perex_by_q.keys()))):
        agg_list = sorted(set(agg_by_q.get(q, [])))
        per_list = sorted(set(perex_by_q.get(q, [])))
        if PREFER_AGGREGATED and agg_list:
            chosen.extend(agg_list)
            chosen_mode[q] = 'agg'
        elif per_list:
            chosen.extend(per_list)
            chosen_mode[q] = 'perex'

    if chosen:
        modes = ", ".join(f"{q}:{chosen_mode[q]}" for q in sorted(chosen_mode))
        print(f"[RESOLVE] {base_raw.upper()} -> quotes [{modes}] | total_symbols={len(sorted(set(chosen)))}")
        return sorted(set(chosen)), "quoteaware"

    # Rescue: regex scan, then apply same per-quote logic
    base_up = base_raw.upper()
    quote_alt = "|".join(_QUOTE_SUFFIXES)
    rx = re.compile(rf"^{re.escape(base_up)}(?:{quote_alt}).*_PERP", re.IGNORECASE)
    hits = [m.get("symbol") for m in markets if rx.match(m.get("symbol",""))]
    if hits:
        agg_by_q.clear(); perex_by_q.clear()
        for h in hits:
            q = _quote_from_symbol(h)
            if h.endswith(".A"): agg_by_q[q].append(h)
            else: perex_by_q[q].append(h)
        chosen = []
        chosen_mode = {}
        for q in sorted(set(list(agg_by_q.keys()) + list(perex_by_q.keys()))):
            a = sorted(set(agg_by_q.get(q, [])))
            p = sorted(set(perex_by_q.get(q, [])))
            if PREFER_AGGREGATED and a:
                chosen.extend(a); chosen_mode[q]='agg'
            elif p:
                chosen.extend(p); chosen_mode[q]='perex'
        if chosen:
            modes = ", ".join(f"{q}:{chosen_mode[q]}" for q in sorted(chosen_mode))
            print(f"[INFO] RESCUE {base_up} -> quotes [{modes}] | total_symbols={len(sorted(set(chosen)))}")
            return sorted(set(chosen)), "rescue-quoteaware"

    return None, "none"

# -------- Liquidations fetch + accumulation --------
def liq_last_hour_by_base(base_to_symbols):
    frm,to=last_completed_hour_window()
    all_syms=sorted({s for syms in base_to_symbols.values() for s in syms})
    symbol_to_base={s:b for b,syms in base_to_symbols.items() for s in syms}

    totals={b:0.0 for b in base_to_symbols}
    longs={b:0.0 for b in base_to_symbols}
    shorts={b:0.0 for b in base_to_symbols}

    # Diagnostics
    raw_by_base={}
    per_symbol_breakdown=defaultdict(list)  # base -> list of {'symbol','quote','ex','l','s','t'}
    by_quote=defaultdict(lambda: defaultdict(lambda: {"l":0.0,"s":0.0}))
    by_exch =defaultdict(lambda: defaultdict(lambda: {"l":0.0,"s":0.0}))

    if not all_syms:
        print("[INFO] No symbols to query in Coinalyze.")
        return totals,longs,shorts,raw_by_base,per_symbol_breakdown,by_quote,by_exch,frm,to

    CHUNK=COINALYZE_CHUNK
    for i in range(0,len(all_syms),CHUNK):
        chunk=all_syms[i:i+CHUNK]
        try:
            _coinalyze_rate_gate()
            data=coinalyze_get("/liquidation-history",{
                "symbols":",".join(chunk),
                "interval":"1hour",
                "from":frm,
                "to":to-1,                 # end exclusive
                "convert_to_usd":"true"})
        except Exception as e:
            print(f"[ERROR] Coinalyze batch failed (size={len(chunk)}): {e}")
            if len(chunk)>1:
                mid=len(chunk)//2
                for sub in (chunk[:mid],chunk[mid:]):
                    try:
                        _coinalyze_rate_gate()
                        data_sub=coinalyze_get("/liquidation-history",{
                            "symbols":",".join(sub),
                            "interval":"1hour",
                            "from":frm,
                            "to":to-1,
                            "convert_to_usd":"true"})
                        _accumulate_liqs(data_sub,symbol_to_base,raw_by_base,per_symbol_breakdown,by_quote,by_exch,totals,longs,shorts,frm,to)
                    except Exception as e2:
                        print(f"[ERROR] Sub-chunk failed (size={len(sub)}): {e2}")
            continue
        _accumulate_liqs(data,symbol_to_base,raw_by_base,per_symbol_breakdown,by_quote,by_exch,totals,longs,shorts,frm,to)
        _sleep_jitter(PACE_SECONDS)

    return totals,longs,shorts,raw_by_base,per_symbol_breakdown,by_quote,by_exch,frm,to

def _accumulate_liqs(data,symbol_to_base,raw_by_base,per_symbol_breakdown,by_quote,by_exch,totals,longs,shorts,frm,to):
    for entry in data:
        sym = entry.get("symbol")
        base_key=symbol_to_base.get(sym)
        if not base_key:
            continue
        hist=entry.get("history",[])
        if not hist:
            continue

        # Choose candle
        target=None; latest_lt=None
        for c in hist:
            try:
                t=int(c.get("t",0))
            except:
                continue
            if t==frm:
                target=c; break
            if t<to:
                if (latest_lt is None) or (t>int(latest_lt.get("t",0))):
                    latest_lt=c
        if target is None and not EXACT_BAR_REQUIRED:
            target = latest_lt

        raw_by_base.setdefault(base_key,[]).extend(hist)
        if target is None:
            print(f"[SKIP] {sym} has no suitable candle (exact={EXACT_BAR_REQUIRED}) at window; excluded")
            continue

        l=float(target.get("l",0)); s=float(target.get("s",0))
        totals[base_key]+=l+s
        longs[base_key]+=l
        shorts[base_key]+=s

        q=_quote_from_symbol(sym)
        ex=_exchange_from_symbol(sym)
        per_symbol_breakdown[base_key].append({
            "symbol": sym, "quote": q, "ex": ex, "l": l, "s": s, "t": int(target.get("t",0))
        })
        by_quote[base_key][q]["l"] += l; by_quote[base_key][q]["s"] += s
        by_exch [base_key][ex]["l"] += l; by_exch [base_key][ex]["s"] += s

# -------- Main --------
_seen_alerts=set()

def run_once():
    coins=get_coins_in_cap_band_sorted()

    # Build quote-aware symbol maps
    agg, perex, markets = group_perps_by_base_quote_both()

    base_to_symbols={}; unmatched=[]
    for coin in coins:
        base_raw=(coin["symbol"] or ""); base=base_raw.lower()
        if base in STABLE_BASES: continue

        if base in OVERRIDES:
            syms=OVERRIDES[base]; syms=[syms] if isinstance(syms,str) else list(syms)
            base_to_symbols[base]=sorted(set(syms))
            print(f"[RESOLVE] {base_raw.upper()} -> OVERRIDE {len(base_to_symbols[base])} symbols")
            continue

        syms, how = resolve_syms_for_coin_quoteaware(coin, agg, perex, markets)
        if syms:
            base_to_symbols[base] = syms
        else:
            unmatched.append(f"{base_raw.upper()} ({coin['name']}) — no perps")

    totals,longs,shorts,raw_by_base,per_symbol_breakdown,by_quote,by_exch,frm,to=liq_last_hour_by_base(base_to_symbols)
    checked=0; alerted=0

    print(f"[INFO] Window {frm}->{to} ({datetime.fromtimestamp(frm, timezone.utc)} – {datetime.fromtimestamp(to, timezone.utc)} UTC)")

    for coin in coins:
        base_raw=(coin["symbol"] or ""); base=base_raw.lower()
        if base in STABLE_BASES: continue
        syms=base_to_symbols.get(base)
        if not syms: continue

        # Production total (quote-aware selection)
        liq_usd=float(totals.get(base,0.0))
        liq_l=float(longs.get(base,0.0))
        liq_s=float(shorts.get(base,0.0))
        if liq_usd<MIN_LIQ_USD: continue
        checked+=1

        # UI emulation (logs only)
        ui_l=ui_s=0.0
        if base in per_symbol_breakdown:
            for row in per_symbol_breakdown[base]:
                if UI_EMU_QUOTES and row["quote"] not in UI_EMU_QUOTES:
                    continue
                if UI_EMU_EXCHANGES and row["ex"] not in UI_EMU_EXCHANGES:
                    continue
                ui_l += row["l"]; ui_s += row["s"]
        ui_total = ui_l + ui_s

        # Math-impossible check before CG /range
        if liq_usd < MIN_LIQ_FOR_POSSIBLE_ALERT:
            print(f"[DEBUG] {base_raw.upper()} skipped (liq < min for any alert) | liq={liq_usd:.2f} need>={MIN_LIQ_FOR_POSSIBLE_ALERT:.2f}")
            continue

        cg_id=BASE_TO_CGID.get(base,coin["id"])
        mc_close=get_market_cap_at_close(cg_id,to)
        if mc_close<=0 or not(LOWER_CAP<=mc_close<=UPPER_CAP):
            print(f"[DEBUG] {base_raw.upper()} dropped by historical MC band | mc_close={mc_close:.2f}")
            continue

        ratio=liq_usd/mc_close

        # ----- LOGS: per-symbol + per-quote + per-exchange -----
        br_list = per_symbol_breakdown.get(base, [])
        if br_list:
            # Determine mode per quote
            q_modes = defaultdict(set)
            for r in br_list:
                q_modes[r['quote']].add('AGG' if r['ex']=='A' else 'PEREX')
            modes = ", ".join(f"{q}:{'/'.join(sorted(q_modes[q]))}" for q in sorted(q_modes))
            print(f"[BRK] {base_raw.upper()} symbols used ({modes}) for {datetime.fromtimestamp(frm, timezone.utc)}–{datetime.fromtimestamp(to, timezone.utc)} UTC:")
            for br in sorted(br_list, key=lambda x: (x["quote"], x["ex"], x["symbol"])):
                print(f"       {br['symbol']:<30} q={br['quote']:<5} ex={br['ex']:<8} long={br['l']:.2f} short={br['s']:.2f} total={br['l']+br['s']:.2f}")

            if by_quote.get(base):
                print(f"[SUMQ] {base_raw.upper()} by quote:")
                for q,vals in by_quote[base].items():
                    print(f"       {q:<5} long={vals['l']:.2f} short={vals['s']:.2f} total={(vals['l']+vals['s']):.2f}")

            if by_exch.get(base):
                print(f"[SUMX] {base_raw.upper()} by exchange:")
                for ex,vals in by_exch[base].items():
                    print(f"       {ex:<8} long={vals['l']:.2f} short={vals['s']:.2f} total={(vals['l']+vals['s']):.2f}")

            if UI_EMU_QUOTES or UI_EMU_EXCHANGES:
                print(f"[UIEMU] {base_raw.upper()} UI-emulated sum "
                      f"(quotes={UI_EMU_QUOTES or 'ALL'}, exchanges={UI_EMU_EXCHANGES or 'ALL'}) "
                      f"long={ui_l:.2f} short={ui_s:.2f} total={ui_total:.2f}")

        print(f"[DEBUG] {base_raw.upper()} | PROD liq={liq_usd:.2f} (L={liq_l:.2f} S={liq_s:.2f}) "
              f"| MC_close={mc_close:.2f} | Liq/MC={ratio*100:.4f}% | syms={syms}")

        if ratio>=RATIO_THRESHOLD:
            key=idem_key(sorted(syms),frm,to,round(ratio,8))
            if key in _seen_alerts: continue
            _seen_alerts.add(key)
            window=(f"{datetime.fromtimestamp(frm, tz=timezone.utc):%Y-%m-%d %H:%M}"
                    f"–{datetime.fromtimestamp(to, tz=timezone.utc):%H:%M} UTC")
            msg=(f"🔔 Liq/MC ≥ {RATIO_THRESHOLD*100:.3f}% (at close)\n"
                 f"Ticker: {', '.join(syms)}\n"
                 f"Window: {window}\n"
                 f"Liquidations: {fmt_usd(liq_usd)} "
                 f"(long {fmt_usd(liq_l)}, short {fmt_usd(liq_s)})\n"
                 f"MC (close): {fmt_usd(mc_close)}\n"
                 f"Liq/MC: {ratio*100:.4f}%")
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
