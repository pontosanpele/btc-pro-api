import requests

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

BINANCE_URL_TICKER_24H = f"{BINANCE_BASE}/api/v3/ticker/24hr"
BINANCE_URL_BOOK_TICKER = f"{BINANCE_BASE}/api/v3/ticker/bookTicker"
BINANCE_URL_KLINES = f"{BINANCE_BASE}/api/v3/klines"

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
    "neutral":            {"flow": 0.21, "volume": 0.16, "structure": 0.22, "execution": 0.20, "context": 0.21},
    "impulse_up":         {"flow": 0.25, "volume": 0.20, "structure": 0.24, "execution": 0.16, "context": 0.15},
    "impulse_down":       {"flow": 0.25, "volume": 0.20, "structure": 0.24, "execution": 0.16, "context": 0.15},
    "trend_build_long":   {"flow": 0.24, "volume": 0.18, "structure": 0.24, "execution": 0.17, "context": 0.17},
    "trend_build_short":  {"flow": 0.24, "volume": 0.18, "structure": 0.24, "execution": 0.17, "context": 0.17},
    "short_squeeze":      {"flow": 0.28, "volume": 0.18, "structure": 0.22, "execution": 0.16, "context": 0.16},
    "long_flush":         {"flow": 0.28, "volume": 0.18, "structure": 0.22, "execution": 0.16, "context": 0.16},
}


# V21 scoring refinements
SESSION_PENALTY_PROFILE = {
    "weekend": {"vol5_warn": 0.18, "vol15_warn": 0.28, "base_penalty": 10},
    "europe":  {"vol5_warn": 0.35, "vol15_warn": 0.45, "base_penalty": 2},
    "us":      {"vol5_warn": 0.45, "vol15_warn": 0.55, "base_penalty": 0},
    "asia":    {"vol5_warn": 0.28, "vol15_warn": 0.38, "base_penalty": 4},
}
SOFT_SCORE_WEIGHTS_V2 = {
    "expected_value": 0.22,
    "entry_timing": 0.14,
    "execution": 0.14,
    "setup_readiness_v2": 0.16,
    "retest_quality": 0.14,
    "breakout_quality_v2": 0.10,
    "trigger_behavior": 0.10,
}
ORDERFLOW_CONFIDENCE_V2 = {
    "delta_weight": 0.30,
    "orderflow_consistency_weight": 0.34,
    "delta_strength_weight": 0.18,
    "flow_alignment_weight": 0.18,
}
