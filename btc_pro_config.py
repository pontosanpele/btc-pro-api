@@ -3,95 +3,96 @@ import requests
BASE = "https://api.bybit.com"
BINANCE_BASE = "https://api.binance.com"
SYMBOL_PERP = "BTCUSDT"
SYMBOL_SPOT = "BTCUSDT"

URL_TICKERS = f"{BASE}/v5/market/tickers"
URL_OI = f"{BASE}/v5/market/open-interest"
URL_FUNDING = f"{BASE}/v5/market/funding/history"
URL_ORDERBOOK = f"{BASE}/v5/market/orderbook"
URL_KLINE = f"{BASE}/v5/market/kline"
URL_TRADES = f"{BASE}/v5/market/recent-trade"
URL_INSTRUMENTS = f"{BASE}/v5/market/instruments-info"
URL_GLOBAL = "https://api.coingecko.com/api/v3/global"
WS_PUBLIC = "wss://stream.bybit.com/v5/public/linear"

DERIBIT_BASE = "https://www.deribit.com/api/v2"
DERIBIT_URL_BOOK_SUMMARY_BY_CURRENCY = f"{DERIBIT_BASE}/public/get_book_summary_by_currency"
DERIBIT_URL_BOOK_SUMMARY_BY_INSTRUMENT = f"{DERIBIT_BASE}/public/get_book_summary_by_instrument"
DERIBIT_BTC_PERP = "BTC-PERPETUAL"
CME_BTC_VOLUME_OI_URL = "https://www.cmegroup.com/markets/cryptocurrencies/bitcoin/bitcoin.volume.html"
CME_BTC_BENCHMARK_URL = "https://www.cmegroup.com/markets/cryptocurrencies/cme-cf-cryptocurrency-benchmarks.html"

BINANCE_URL_TICKER_24H = f"{BINANCE_BASE}/api/v3/ticker/24hr"
BINANCE_URL_BOOK_TICKER = f"{BINANCE_BASE}/api/v3/ticker/bookTicker"
BINANCE_URL_KLINES = f"{BINANCE_BASE}/api/v3/klines"
BINANCE_URL_DEPTH = f"{BINANCE_BASE}/api/v3/depth"

HISTORY_PATH = "~/btc_snapshot_history.jsonl"
MAX_HISTORY_ROWS = 120
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "btc-pro-v27-source-priority/1.0"})

def f(x, default=None):
    try:
        if x in (None, "", "None"): return default
        return float(x)
    except Exception:
        return default


def pct(new, old, default=None):
    if new is None or old in (None, 0): return default
    return (new - old) / old * 100.0


def safe_div(a, b, default=None):
    if a is None or b in (None, 0): return default
    return a / b


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def fmt_side(side):
    if side in ("Buy", "buy", "B"): return "Buy"
    if side in ("Sell", "sell", "S"): return "Sell"
    return side


# V27 data-source routing policy
PERP_PRIMARY_EXCHANGE = "bybit"
SPOT_PRIMARY_EXCHANGE = "binance"
FIELD_SOURCE_PRIORITY = {
    "perp_last": ["bybit"],
    "perp_mark": ["bybit"],
    "perp_index": ["bybit"],
    "funding": ["bybit"],
    "open_interest": ["bybit"],
    "recent_trades": ["bybit"],
    "orderbook": ["bybit"],
    "orderbook": ["bybit", "binance"],
    "instrument_info": ["bybit"],
    "spot_ticker": ["binance", "bybit"],
    "spot_book": ["binance", "bybit"],
    "spot_klines": ["binance", "bybit"],
}
STALE_SECONDS_BY_FIELD = {
    "spot_ticker": 8,
    "spot_book": 8,
    "perp_last": 5,
    "orderbook": 4,
    "recent_trades": 4,
}


# V19 normalization / scoring config
NOISE_FLOOR = {
    "delta_pct": 4.0,
    "oi_pct": 0.08,
    "spot_perp_div_pct_pt": 0.015,
    "trigger_distance_pct": 0.03,
    "orderbook_imbalance_pct": 3.5,
    "price_vs_vwap_pct": 0.05,
}
REGIME_WEIGHT_PROFILE = {
    "low_liquidity_range": {"flow": 0.20, "volume": 0.14, "structure": 0.20, "execution": 0.22, "context": 0.24},
