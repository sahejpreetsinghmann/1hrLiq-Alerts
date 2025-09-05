import time, requests
from datetime import datetime, timedelta, timezone

# ====== YOUR TELEGRAM + COINALYZE DETAILS ======
CHAT_IDS = ["-4869615280"]  # Telegram group
COINALYZE_KEY = "30d603dd-9814-421e-94cc-62f9775c541c"
TELEGRAM_TOKEN = "8422686073:AAGmMzABWh9r8cyXrdpoWYldThb51AaK0Aw"
# ===============================================

# Settings
LOWER_CAP = 50_000_000        # $50M
UPPER_CAP = 500_000_000       # $500M
RATIO_THRESHOLD = 0.0002      # 0.02% of market cap
MIN_LIQ_USD = 0
PACE_SECONDS = 1.7            # ~35 calls/min for Coinalyze
SEND_NO_HITS_SUMMARY = True

# CoinGecko rate-limit handling
COINGECKO_PACE_SECONDS = 1.8  # pause between CG pages
MAX_RETRIES = 7               # total attempts per request
INITIAL_COOLDOWN = 3.0        # pause before first CG request of the run

# Manual overrides: CoinGecko symbol (lowercase) → Coinalyze aggregated perp symbol
OVERRIDES = {
    # "wbtc": "BTCUSDT_PERP.A",
    # "lunc": "LUNAUSDT_PERP.A",
}

# Endpoints
COINALYZE_BASE = "https://api.coinalyze.net/v1"
TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
CG_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"

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

# ---------- Robust CoinGecko GET with backoff ----------
def http_get_with_backoff(url, params=None, timeout=40):
    headers = {"User-Agent": "liq-alerts/1.0 (+github.com/yourrepo)"}
    wait = 0.0
    for attempt in range(1, MAX_RETRIES + 1):
        if wait > 0:
            time.sleep(wait)
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            # 429: Too Many Requests -> backoff and retry
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                # If server suggests a wait, use it; else exponential backoff
                wait = float(ra) if (ra and ra.isdigit()) else min(2 * attempt, 30)
                continue
            # 5xx: transient -> backoff and retry
            if 500 <= r.status_code < 600:
                wait = min(2 * attempt, 30)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException:
            # network/dns/timeout etc -> backoff and retry
            wait = min(2 * attempt, 30)
            continue
    # If we exhausted retries, raise a friendly error
    raise RuntimeError("CoinGecko request repeatedly rate-limited or failed; try again shortly.")

def get_future_markets():
    data = coinalyze_get("/future-markets", {})
    out = []
    for m in data:
        sym = m.get("symbol", "")
        base = (m.get("base_asset") or "").lower()
        if "_PERP" in sym and sym.endswith(".A"):
            out.append({"symbol": sym, "base": base})
    return out

def map_base_to_agg_symbol(future_markets):
    mapping = {}
    for m in future_markets:
        b = m["base"]
        mapping.setdefault(b, m["symbol"])
    return mapping

def get_coins_in_cap_band_sorted():
    """
    Get all coins in the $50M–$500M cap band.
    Sort by |1h move| desc; if 1h missing, use 24h; if both missing, 0.
    """
    time.sleep(INITIAL_COOLDOWN)  # cool down before first CG call
    coins = []
    page = 1
    while True:
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "price_change_percentage": "1h,24h",
        }
        r = http_get_with_backoff(CG_MARKETS, params=params, timeout=45)
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
                one_h = c.get("price_change_percentage_1h_in_currency")
                day = c.get("price_change_percentage_24h_in_currency")
                move = one_h if (one_h is not None) else day
                move_score = abs(move) if (move is not None) else 0.0
                coins.append({
                    "id": c.get("id"),
                    "symbol": (c.get("symbol") or "").lower(),
                    "name": c.get("name"),
                    "market_cap": mc,
                    "move_score": move_score
                })

        if stop:
            break

        page += 1
        if page > 20:  # hard stop safety
            break

        time.sleep(COINGECKO_PACE_SECONDS)  # pacing between pages

    coins.sort(key=lambda x: x["move_score"], reverse=True)
    return coins

def get_last_hour_liqs_usd(symbol):
    frm, to = last_completed_hour_window()
    data = coinalyze_get("/liquidation-history", {
        "symbols": symbol,
        "interval": "1hour",
        "from": frm,
        "to": to - 1,
        "convert_to_usd": "true",
    })
    hist = (data or [{}])[0].get("history", [])
    if not hist:
        return 0.0, frm, to
    c = hist[-1]
    liq_usd = float(c.get("l", 0)) + float(c.get("s", 0))
    return liq_usd, frm, to

def run_once():
    coins = get_coins_in_cap_band_sorted()
    futures = get_future_markets()
    base_to_sym = map_base_to_agg_symbol(futures)

    unmatched = []
    checked = 0
    alerted = 0

    for coin in coins:
        base = coin["symbol"]
        mc = coin["market_cap"]

        # Apply overrides first
        sym = OVERRIDES.get(base) or base_to_sym.get(base)

        if not sym:
            unmatched.append(f"{coin['symbol'].upper()} ({coin['name']}) — no Coinalyze perp found")
            continue

        liq_usd, frm, to = get_last_hour_liqs_usd(sym)
        checked += 1

        if liq_usd >= MIN_LIQ_USD and mc and mc > 0:
            ratio = liq_usd / mc
            if ratio >= RATIO_THRESHOLD:
                window = (f"{datetime.fromtimestamp(frm, tz=timezone.utc):%Y-%m-%d %H:%M}"
                          f"–{datetime.fromtimestamp(to, tz=timezone.utc):%H:%M} UTC")
                msg = (f"🔔 Liq/MC ≥ {RATIO_THRESHOLD*100:.3f}%\n"
                       f"Ticker: {sym}\n"
                       f"Window: {window}\n"
                       f"Liquidations: {fmt_usd(liq_usd)}\n"
                       f"Market Cap:  {fmt_usd(mc)}\n"
                       f"Liq/MC: {ratio*100:.3f}%")
                send_tg(msg)
                alerted += 1

        time.sleep(PACE_SECONDS)  # respect Coinalyze limit

    if unmatched:
        head = "⚠️ Coins in $50–$500M cap band without a Coinalyze aggregated perp:"
        lines = [head] + unmatched[:25]
        if len(unmatched) > 25:
            lines.append(f"...and {len(unmatched)-25} more")
        send_tg("\n".join(lines))

    if SEND_NO_HITS_SUMMARY and alerted == 0:
        send_tg(f"ℹ️ Scan done: checked {checked} perps; no Liq/MC >= {RATIO_THRESHOLD*100:.3f}% in last hour.")

if __name__ == "__main__":
    run_once()
