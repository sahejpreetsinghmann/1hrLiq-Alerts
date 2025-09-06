# liq_alerts_v15_allquotes_robust_namedex.py
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

# Candle alignment: require exact bar at t==frm? If False, fall back to latest < to
EXACT_BAR_REQUIRED = False

# Manual overrides (base symbol -> list of Coinalyze symbols to use)
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

# Stables to exclude from base list (WUSDT intentionally NOT included)
STABLE_BASES = {"usdt","usdc","usd","susd","gusd","tusd","dai","usde","usdp","usdd","usds","usdx"}

# -------- Endpoints --------
COINALYZE_BASE = "https://api.coinalyze.net/v1"
TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
CG_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
CG_RANGE   = "https://api.coingecko.com/api/v3/coins/{id}/market_chart/range"

# -------- HTTP helpers --------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "liq-alerts/v15-allquotes-namedex"})

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
_QUOTES = ["USDT","USD","USDC","BUSD","FDUSD","USDE","TUSD","USDP","USDD","USDX"]

def _base_from_symbol(sym: str) -> str:
    head = sym.split("_PERP", 1)[0]
    for q in _QUOTES:
        if head.endswith(q):
            return head[:-len(q)]
    return head

def _quote_from_symbol(sym: str) -> str:
    head = sym.split("_PERP", 1)[0]
    for q in _QUOTES:
        if head.endswith(q):
            return q
    return "UNKNOWN"

# -------- MARKET COLLECTION (robust + exchange labels) --------
def fetch_all_markets_with_exlabels():
    markets = coinalyze_get("/future-markets", {})
    sym_to_exlabel = {}
    for m in markets:
        sym = m.get("symbol","")
        if not sym: continue
        # If it's aggregated, label as AGG
        if sym.endswith(".A"):
            sym_to_exlabel[sym] = "AGG"
            continue

        # Try explicit fields first (names vary by API version)
        ex_fields = [
            m.get("exchange"),
            m.get("exchange_name"),
            m.get("exchange_display"),
            m.get("venue"),
        ]
        exlabel = next((x for x in ex_fields if isinstance(x, str) and x.strip()), None)

        # If not found, try using exchange_id
        if not exlabel:
            ex_id = m.get("exchange_id") or m.get("venue_id")
            if isinstance(ex_id, (int, str)) and str(ex_id).strip():
                exlabel = f"EXCH-{ex_id}"

        # Fallback: parse from the symbol suffix after the dot
        if not exlabel:
            if "." in sym:
                tail = sym.split(".")[-1]
                if tail.upper() != "A":
                    exlabel = tail if tail else "UNKNOWN"
            else:
                exlabel = "UNKNOWN"

        sym_to_exlabel[sym] = exlabel

    return markets, sym_to_exlabel

def collect_symbols_for_coin(coin, markets):
    """
    Robust: scan the full markets list and pick every perp whose base matches
    the coin's symbol OR name OR id (normalized), using BOTH:
      - m['base_asset'], and
      - parsed base from 'symbol' (e.g., DOLO from DOLOUSDT_PERP.*)
    Returns:
      by_quote = { "USDT": {"agg":[...], "per":[...]}, ... }
    """
    base_raw = (coin.get("symbol") or "")
    keys = {norm(base_raw), norm(coin.get("name","")), norm(coin.get("id",""))}
    by_quote = defaultdict(lambda: {"agg": set(), "per": set()})

    for m in markets:
        sym = m.get("symbol","")
        if "_PERP" not in sym:
            continue
        q = _quote_from_symbol(sym)
        if q == "UNKNOWN":
            continue

        b_asset = norm(m.get("base_asset",""))
        b_sym   = norm(_base_from_symbol(sym))

        if (b_asset in keys) or (b_sym in keys):
            if sym.endswith(".A"):
                by_quote[q]["agg"].add(sym)
            else:
                by_quote[q]["per"].add(sym)

    # Rescue: if none found, try regex on base symbol
    if not any(by_quote.values()) and base_raw:
        base_up = base_raw.upper()
        qalt = "|".join(_QUOTES)
        rx = re.compile(rf"^{re.escape(base_up)}(?:{qalt}).*_PERP", re.IGNORECASE)
        for m in markets:
            sym = m.get("symbol","")
            if "_PERP" not in sym: continue
            if not rx.match(sym): continue
            q = _quote_from_symbol(sym)
            if sym.endswith(".A"):
                by_quote[q]["agg"].add(sym)
            else:
                by_quote[q]["per"].add(sym)

    # to sorted lists
    out = {}
    for q,parts in by_quote.items():
        agg = sorted(parts["agg"])
        per = sorted(parts["per"])
        if agg or per:
            out[q] = {"agg": agg, "per": per}
    return out

# -------- Liquidations helpers --------
def _select_candle(hist, frm, to):
    target=None; latest_lt=None
    for c in hist:
        try:
            t=int(c.get("t",0))
        except:
            continue
        if t==frm:
            return c
        if t<to:
            if (latest_lt is None) or (t>int(latest_lt.get("t",0))):
                latest_lt=c
    return target if EXACT_BAR_REQUIRED else latest_lt

def fetch_liqs_for_symbols(symbols, frm, to):
    """Return list of rows: {'symbol','l','s','t','quote'} for the selected candle per symbol."""
    rows = []
    if not symbols: return rows
    CHUNK = COINALYZE_CHUNK
    for i in range(0, len(symbols), CHUNK):
        chunk = symbols[i:i+CHUNK]
        try:
            _coinalyze_rate_gate()
            data=coinalyze_get("/liquidation-history",{
                "symbols":",".join(chunk),
                "interval":"1hour",
                "from":frm,
                "to":to-1,
                "convert_to_usd":"true"})
        except Exception as e:
            print(f"[ERROR] Coinalyze batch failed (size={len(chunk)}): {e}")
            continue
        for entry in data:
            sym = entry.get("symbol")
            hist = entry.get("history", [])
            if not hist: continue
            target = _select_candle(hist, frm, to)
            if target is None:
                print(f"[SKIP] {sym} has no suitable candle (exact={EXACT_BAR_REQUIRED})")
                continue
            rows.append({
                "symbol": sym,
                "l": float(target.get("l",0)),
                "s": float(target.get("s",0)),
                "t": int(target.get("t",0)),
                "quote": _quote_from_symbol(sym),
            })
        _sleep_jitter(PACE_SECONDS)
    return rows

# -------- Production accumulation (quote-aware, no double count) --------
def liq_last_hour_quoteaware(diag_by_quote):
    """
    diag_by_quote: {quote: {"agg":[syms], "per":[syms]}}
    Returns production totals + per-symbol rows used.
    """
    frm,to=last_completed_hour_window()
    used_symbols = []
    for q, parts in diag_by_quote.items():
        if parts["agg"]:
            used_symbols.extend(parts["agg"])
        elif parts["per"]:
            used_symbols.extend(parts["per"])

    rows = fetch_liqs_for_symbols(sorted(set(used_symbols)), frm, to)

    totals = {"sum":0.0,"L":0.0,"S":0.0}
    for r in rows:
        l,s = r["l"], r["s"]
        totals["sum"] += l+s
        totals["L"]   += l
        totals["S"]   += s

    return totals, rows, frm, to

# -------- Diagnostics: log .A vs per-ex per quote + exchange splits --------
def log_diag_compare(base_raw, diag_by_quote, sym_to_exlabel, frm, to):
    if not diag_by_quote:
        print(f"[DIAG] {base_raw.upper()} no perp symbols found for diagnostics")
        return
    for q in sorted(diag_by_quote.keys()):
        agg_syms = diag_by_quote[q].get("agg", [])
        per_syms = diag_by_quote[q].get("per", [])

        agg_rows = fetch_liqs_for_symbols(agg_syms, frm, to) if agg_syms else []
        per_rows = fetch_liqs_for_symbols(per_syms, frm, to) if per_syms else []

        # Attach exchange labels
        def ex_of(sym): return sym_to_exlabel.get(sym, "UNKNOWN")

        agg_L = sum(r["l"] for r in agg_rows); agg_S = sum(r["s"] for r in agg_rows)
        per_L = sum(r["l"] for r in per_rows); per_S = sum(r["s"] for r in per_rows)

        print(f"[DIAG] {base_raw.upper()} q={q}: "
              f".A long={agg_L:.2f} short={agg_S:.2f} total={(agg_L+agg_S):.2f}  ||  "
              f"PER-EX long={per_L:.2f} short={per_S:.2f} total={(per_L+per_S):.2f} "
              f"diff={(agg_L+agg_S)-(per_L+per_S):.2f}")

        # Detailed lines (clear exchange names)
        if agg_rows:
            print(f"[DIAG-A] {base_raw.upper()} q={q} aggregated components:")
            for r in agg_rows:
                print(f"         {r['symbol']:<32} ex={ex_of(r['symbol']):<10} "
                      f"L={r['l']:.2f} S={r['s']:.2f} T={(r['l']+r['s']):.2f}")

        if per_rows:
            print(f"[DIAG-P] {base_raw.upper()} q={q} per-exchange components:")
            by_ex = defaultdict(lambda: {"l":0.0,"s":0.0})
            for r in per_rows:
                ex = ex_of(r["symbol"])
                by_ex[ex]["l"] += r["l"]; by_ex[ex]["s"] += r["s"]
                print(f"         {r['symbol']:<32} ex={ex:<10} "
                      f"L={r['l']:.2f} S={r['s']:.2f} T={(r['l']+r['s']):.2f}")
            print(f"[DIAGX] {base_raw.upper()} q={q} per-exchange totals:")
            for ex,vals in sorted(by_ex.items()):
                tot = vals["l"] + vals["s"]
                print(f"         ex={ex:<10} L={vals['l']:.2f} S={vals['s']:.2f} T={tot:.2f}")

# -------- Main --------
_seen_alerts=set()

def run_once():
    coins=get_coins_in_cap_band_sorted()
    markets, sym_to_exlabel = fetch_all_markets_with_exlabels()

    checked=0; alerted=0
    unmatched=[]

    for coin in coins:
        base_raw=(coin["symbol"] or ""); base=base_raw.lower()
        if base in STABLE_BASES:
            continue

        # Build comprehensive symbol set for this coin
        if base in OVERRIDES:
            diag_by_quote = defaultdict(lambda: {"agg": [], "per": []})
            syms = OVERRIDES[base]
            syms = [syms] if isinstance(syms,str) else list(syms)
            for s in syms:
                (diag_by_quote[_quote_from_symbol(s)]["agg" if s.endswith(".A") else "per"]).append(s)
            print(f"[RESOLVE] {base_raw.upper()} -> OVERRIDE {len(syms)} symbols")
        else:
            diag_by_quote = collect_symbols_for_coin(coin, markets)

        if not diag_by_quote:
            unmatched.append(f"{base_raw.upper()} ({coin['name']}) — no perps")
            continue

        # PRODUCTION (no double counting): per quote use .A if exists else per-ex
        totals, used_rows, frm, to = liq_last_hour_quoteaware(diag_by_quote)
        liq_usd, liq_l, liq_s = totals["sum"], totals["L"], totals["S"]

        if liq_usd < MIN_LIQ_USD:
            continue
        checked += 1

        # Pre-check before CG /range
        if liq_usd < MIN_LIQ_FOR_POSSIBLE_ALERT:
            print(f"[DEBUG] {base_raw.upper()} skipped (liq < min for any alert) | liq={liq_usd:.2f} need>={MIN_LIQ_FOR_POSSIBLE_ALERT:.2f}")
            print(f"[INFO] Window {frm}->{to} ({datetime.fromtimestamp(frm, timezone.utc)} – {datetime.fromtimestamp(to, timezone.utc)} UTC)")
            log_diag_compare(base_raw, diag_by_quote, sym_to_exlabel, frm, to)
            continue

        # Historical market cap at the candle close
        cg_id=BASE_TO_CGID.get(base,coin["id"])
        mc_close=get_market_cap_at_close(cg_id,to)
        if mc_close<=0 or not(LOWER_CAP<=mc_close<=UPPER_CAP):
            print(f"[DEBUG] {base_raw.upper()} dropped by historical MC band | mc_close={mc_close:.2f}")
            print(f"[INFO] Window {frm}->{to} ({datetime.fromtimestamp(frm, timezone.utc)} – {datetime.fromtimestamp(to, timezone.utc)} UTC)")
            log_diag_compare(base_raw, diag_by_quote, sym_to_exlabel, frm, to)
            continue

        ratio=liq_usd/mc_close

        # --- LOGS: production summary ---
        print(f"[INFO] Window {frm}->{to} ({datetime.fromtimestamp(frm, timezone.utc)} – {datetime.fromtimestamp(to, timezone.utc)} UTC)")
        print(f"[DEBUG] {base_raw.upper()} | PROD liq={liq_usd:.2f} (L={liq_l:.2f} S={liq_s:.2f}) "
              f"| MC_close={mc_close:.2f} | Liq/MC={ratio*100:.4f}%")

        # --- LOGS: side-by-side diagnostics with named exchanges ---
        log_diag_compare(base_raw, diag_by_quote, sym_to_exlabel, frm, to)

        if ratio>=RATIO_THRESHOLD:
            key=idem_key(sorted([r["symbol"] for r in used_rows]),frm,to,round(ratio,8))
            if key in _seen_alerts:
                continue
            _seen_alerts.add(key)
            window=(f"{datetime.fromtimestamp(frm, tz=timezone.utc):%Y-%m-%d %H:%M}"
                    f"–{datetime.fromtimestamp(to, tz=timezone.utc):%H:%M} UTC")
            msg=(f"🔔 Liq/MC ≥ {RATIO_THRESHOLD*100:.3f}% (at close)\n"
                 f"Base: {base_raw.upper()}\n"
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
