import time, requests
from datetime import datetime, timedelta, timezone

# ====== TELEGRAM + COINALYZE ======
CHAT_IDS = ["-4869615280"]
COINALYZE_KEY = "30d603dd-9814-421e-94cc-62f9775c541c"
TELEGRAM_TOKEN = "8422686073:AAGmMzABWh9r8cyXrdpoWYldThb51AaK0Aw"
# ===================================

# Settings
LOWER_CAP = 50_000_000
UPPER_CAP = 500_000_000
RATIO_THRESHOLD = 0.0002      # 0.02%
MIN_LIQ_USD = 0
PACE_SECONDS = 1.7             # Coinalyze pacing per request/chunk
SEND_NO_HITS_SUMMARY = True

# CoinGecko pacing/retries
COINGECKO_PACE_SECONDS = 1.8
MAX_RETRIES = 7
INITIAL_COOLDOWN = 3.0         # pause before first CG call

# Manual overrides: CoinGecko symbol (lowercase) → Coinalyze aggregated perp symbol(s)
# You can map to a *list* to force exactly which aggregated perps to sum.
# e.g., "wbtc": ["BTCUSDT_PERP.A"], or "abc": ["ABCUSDT_PERP.A","ABCUSD_PERP.A"]
OVERRIDES = {
    # "wbtc": ["BTCUSDT_PERP.A"],
    # "lunc": ["LUNAUSDT_PERP.A"],
}

# Endpoints
COINALYZE_BASE = "https://api.coinalyze.net/v1"
TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
CG_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
CG_RANGE   = "https://api.coingecko.com/api/v3/coins/{id}/market_chart/range"

def send_tg(text: str):
    for cid in CHAT_IDS:
        try:
            requests.post(f"{TG_BASE}/sendMessage",
                          json={"chat_id": cid, "text": text}, timeout=20)
        except Exception:
            pass

def fmt_usd(x): return f"${x:,.0f}"

def last_completed_hour_window():
    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    return int(start.timestamp()), int(end.timestamp())

def coinalyze_get(path, params):
    params = dict(params or {})
    params["api_key"] = COINALYZE_KEY
    r = requests.get(f"{COINALYZE_BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# ---------- robust CoinGecko GET with backoff ----------
def http_get_with_backoff(url, params=None, timeout=45):
    headers = {"User-Agent": "liq-alerts/1.0 (+github.com/yourrepo)"}
    wait = 0.0
    for attempt in range(1, MAX_RETRIES + 1):
        if wait > 0:
            time.sleep(wait)
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if (ra and ra.isdigit()) else min(2 * attempt, 30)
                continue
            if 500 <= r.status_code < 600:
                wait = min(2 * attempt, 30)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException:
            wait = min(2 * attempt, 30)
            continue
    raise RuntimeError("CoinGecko request repeatedly rate-limited/failed.")

# ---------------- coin list + ordering + dedupe ----------------
def get_coins_in_cap_band_sorted():
    """Return coins in cap band, deduped by symbol (keep highest MC),
       sorted by |1h move| desc (fallback 24h)."""
    time.sleep(INITIAL_COOLDOWN)
    by_symbol = {}  # symbol -> dict( id, symbol, name, market_cap, move_score )
    page = 1
    while True:
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "price_change_percentage": "1h,24h",
        }
        r = http_get_with_backoff(CG_MARKETS, params=params)
        batch = r.json()
        if not batch:
            break

        stop = False
        for c in batch:
            mc = c.get("market_cap")
            if mc is None:
                continue
            if mc < LOWER_CAP:
                stop = True
                break
            if LOWER_CAP <= mc <= UPPER_CAP:
                sym = (c.get("symbol") or "").lower()
                one_h = c.get("price_change_percentage_1h_in_currency")
                day = c.get("price_change_percentage_24h_in_currency")
                move = one_h if (one_h is not None) else day
                move_score = abs(move) if (move is not None) else 0.0
                row = {
                    "id": c.get("id"),
                    "symbol": sym,
                    "name": c.get("name"),
                    "market_cap": mc,
                    "move_score": move_score
                }
                if (sym not in by_symbol) or (mc > by_symbol[sym]["market_cap"]):
                    by_symbol[sym] = row

        if stop:
            break
        page += 1
        if page > 20:
            break
        time.sleep(COINGECKO_PACE_SECONDS)

    coins = list(by_symbol.values())
    coins.sort(key=lambda x: x["move_score"], reverse=True)
    return coins

# ------------- historical MC at candle close -------------
def get_market_cap_at_close(coin_id: str, ts_end: int) -> float:
    """Market cap near the last hour close using CG range API."""
    frm = ts_end - 30*60
    to  = ts_end + 1
    params = {"vs_currency": "usd", "from": frm, "to": to}
    url = CG_RANGE.format(id=coin_id)
    r = http_get_with_backoff(url, params=params)
    js = r.json()
    series = js.get("market_caps") or []
    if not series:
        return 0.0
    target_ms = ts_end * 1000
    best = None
    for ms, val in series:
        if ms <= target_ms:
            best = val
        else:
            break
    if best is None:
        best = series[-1][1]
    return float(best or 0.0)

# ---------------- Coinalyze: group all aggregated perps ----------------
def group_perps_by_base():
    """Returns dict: base -> list of aggregated perp symbols for that base."""
    markets = coinalyze_get("/future-markets", {})
    groups = {}
    for m in markets:
        sym = m.get("symbol","")
        base = (m.get("base_asset") or "").lower()
        if "_PERP" in sym and sym.endswith(".A"):
            groups.setdefault(base, []).append(sym)
    return groups

def get_last_hour_liqs_sum(symbols):
    """Sum last-hour USD liquidations across a list of aggregated perp symbols."""
    if not symbols:
        return 0.0, *last_completed_hour_window()
    frm, to = last_completed_hour_window()
    total = 0.0
    # Batch up to 20 symbols per request (per Coinalyze API spec)
    for i in range(0, len(symbols), 20):
        chunk = symbols[i:i+20]
        data = coinalyze_get("/liquidation-history", {
            "symbols": ",".join(chunk),
            "interval": "1hour",
            "from": frm,
            "to": to - 1,
            "convert_to_usd": "true",
        })
        for entry in data:
            hist = entry.get("history", [])
            if hist:
                c = hist[-1]
                total += float(c.get("l", 0)) + float(c.get("s", 0))
        time.sleep(PACE_SECONDS)  # tiny pace between chunks
    return total, frm, to

# ------------------------- main --------------------------
def run_once():
    coins = get_coins_in_cap_band_sorted()
    base_groups = group_perps_by_base()  # base -> [all aggregated perps]

    unmatched = []
    checked = 0
    alerted = 0

    for coin in coins:
        base = coin["symbol"]
        cg_id = coin["id"]

        # If you provided overrides, they take precedence
        if base in OVERRIDES:
            syms = OVERRIDES[base]
            if isinstance(syms, str):
                syms = [syms]
        else:
            syms = base_groups.get(base, [])

        if not syms:
            unmatched.append(f"{coin['symbol'].upper()} ({coin['name']}) — no Coinalyze aggregated perps found")
            continue

        # Liquidations for last completed hour across ALL aggregated perps for this base
        liq_usd, frm, to = get_last_hour_liqs_sum(syms)
        checked += 1

        if liq_usd < MIN_LIQ_USD:
            continue

        # Market cap AT THE CANDLE CLOSE (use CoinGecko range)
        mc_close = get_market_cap_at_close(cg_id, to)
        if mc_close <= 0:
            continue

        ratio = liq_usd / mc_close
        if ratio >= RATIO_THRESHOLD:
            window = (f"{datetime.fromtimestamp(frm, tz=timezone.utc):%Y-%m-%d %H:%M}"
                      f"–{datetime.fromtimestamp(to, tz=timezone.utc):%H:%M} UTC")
            msg = (f"🔔 Liq/MC ≥ {RATIO_THRESHOLD*100:.3f}% (at close)\n"
                   f"Ticker: {', '.join(syms)}\n"
                   f"Window: {window}\n"
                   f"Liquidations (all perps): {fmt_usd(liq_usd)}\n"
                   f"MC (close):               {fmt_usd(mc_close)}\n"
                   f"Liq/MC: {ratio*100:.3f}%")
            send_tg(msg)
            alerted += 1

    if unmatched:
        head = "⚠️ Coins in $50–$500M cap band without Coinalyze aggregated perps:"
        lines = [head] + unmatched[:25]
        if len(unmatched) > 25:
            lines.append(f"...and {len(unmatched)-25} more")
        send_tg("\n".join(lines))

    if SEND_NO_HITS_SUMMARY and alerted == 0:
        send_tg(f"ℹ️ Scan done: checked {checked} bases; no Liq/MC >= {RATIO_THRESHOLD*100:.3f}% in last hour.")

if __name__ == "__main__":
    run_once()
