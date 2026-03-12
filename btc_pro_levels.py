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
btc_pro_levels.py
btc_pro_levels.py
+86
-12

import math


def liquidity_map_proxy(d):
    above = []
    below = []
    last = d.get('last')
    for key in ['prev_5m_high', 'prev_15m_high', 'swing_high_12x5m', 'vwap_24h']:

    # 15m + 1h szerkezeti/liquidity jelöltek, 1h nagyobb súllyal.
    above_keys = [
        ('prev_5m_high', 1.0),
        ('prev_15m_high', 1.25),
        ('prev_1h_high', 1.85),
        ('swing_high_12x5m', 1.1),
        ('vwap_1h', 1.4),
        ('vwap_24h', 1.8),
    ]
    below_keys = [
        ('prev_5m_low', 1.0),
        ('prev_15m_low', 1.25),
        ('prev_1h_low', 1.85),
        ('swing_low_12x5m', 1.1),
        ('vwap_1h', 1.4),
        ('vwap_24h', 1.8),
    ]

    for key, weight in above_keys:
        v = d.get(key)
        if v is not None and last is not None and v > last:
            above.append((abs(v - last), v))
    for key in ['prev_5m_low', 'prev_15m_low', 'swing_low_12x5m', 'vwap_24h', 'vwap_1h']:
            dist = abs(v - last)
            above.append((dist / weight, v, key))

    for key, weight in below_keys:
        v = d.get(key)
        if v is not None and last is not None and v < last:
            below.append((abs(v - last), v))
            dist = abs(v - last)
            below.append((dist / weight, v, key))

    above.sort(key=lambda x: x[0])
    below.sort(key=lambda x: x[0])

    liq_above_1 = above[0][1] if len(above) > 0 else None
    liq_above_2 = above[1][1] if len(above) > 1 else None
    liq_below_1 = below[0][1] if len(below) > 0 else None
    liq_below_2 = below[1][1] if len(below) > 1 else None

    # HTF célárak külön: 1h/24h alapú likviditás és 15m főbb szintek.
    htf_above_candidates = [
        d.get('vwap_24h'),
        d.get('vwap_1h'),
        d.get('prev_1h_high'),
        d.get('prev_15m_high'),
    ]
    htf_below_candidates = [
        d.get('vwap_24h'),
        d.get('vwap_1h'),
        d.get('prev_1h_low'),
        d.get('prev_15m_low'),
    ]
    htf_above = min([x for x in htf_above_candidates if x is not None and last is not None and x > last], default=None)
    htf_below = max([x for x in htf_below_candidates if x is not None and last is not None and x < last], default=None)

    nearest = 'neutral'
    if last is not None and liq_above_1 is not None and liq_below_1 is not None:
        nearest = 'above' if abs(liq_above_1 - last) < abs(last - liq_below_1) else 'below'

    return {
        'liq_above_1': liq_above_1,
        'liq_above_2': liq_above_2,
        'liq_below_1': liq_below_1,
        'liq_below_2': liq_below_2,
        'liq_above_htf': htf_above,
        'liq_below_htf': htf_below,
        'nearest_liquidity_side': nearest,
        'liq_above_source_1': above[0][2] if above else None,
        'liq_below_source_1': below[0][2] if below else None,
    }


def _safe_ratio(a, b):
    try:
        return a / b if a is not None and b not in (None, 0) else None
    except Exception:
        return None


def orderbook_wall_tracker(d):
    mid = d.get('orderbook_mid')
    bid_wall = d.get('liq_below_1')
    ask_wall = d.get('liq_above_1')
    bid_wall = d.get('largest_bid_wall_price') or d.get('liq_below_1')
    ask_wall = d.get('largest_ask_wall_price') or d.get('liq_above_1')
    ob = d.get('orderbook_imbalance_0_25_pct')
    bid_usd = d.get('largest_bid_wall_usd')
    ask_usd = d.get('largest_ask_wall_usd')
    if bid_usd is None and None not in (mid, bid_wall):
        bid_usd = abs(mid - bid_wall) * 1000
    if ask_usd is None and None not in (mid, ask_wall):
        ask_usd = abs(ask_wall - mid) * 1000

    ratio = _safe_ratio(ask_usd, bid_usd)
    if ratio is None:
        ratio = 1.0

    if ob is None:
        micro = 'neutral'
    elif ob >= 8:
        micro = 'bid'
    elif ob >= 3:
        micro = 'slight_bid'
    elif ob <= -8:
        micro = 'ask'
    elif ob <= -3:
        micro = 'slight_ask'
    else:
        micro = 'balanced'

@@ -103,83 +149,111 @@ def orderbook_wall_tracker(d):
    above_score = 15.0 + (8.0 if micro in ('ask', 'slight_ask') else 0.0) + min(max((ratio - 1.0) * 18.0, 0.0), 25.0)
    below_score = 15.0 + (8.0 if micro in ('bid', 'slight_bid') else 0.0)
    inv = _safe_ratio(bid_usd, ask_usd)
    if inv is not None:
        below_score += min(max((inv - 1.0) * 18.0, 0.0), 25.0)

    return {
        'largest_bid_wall_price': bid_wall,
        'largest_ask_wall_price': ask_wall,
        'largest_bid_wall_usd': bid_usd,
        'largest_ask_wall_usd': ask_usd,
        'micro_orderbook_pressure': micro,
        'major_wall_pressure': major,
        'wall_pressure_side': wall_side,
        'orderbook_wall_explanation': explanation,
        'major_liq_above': ask_wall,
        'major_liq_below': bid_wall,
        'major_liq_above_score': round(above_score, 2),
        'major_liq_below_score': round(below_score, 2),
        'wall_ratio_ask_to_bid': round(ratio, 2),
    }


def trigger_engine(d):
    last = d.get('last')
    bull_trigger = d.get('liq_above_1') or d.get('prev_15m_high') or d.get('prev_5m_high')
    bear_trigger = d.get('liq_below_1') or d.get('prev_15m_low') or d.get('prev_5m_low')
    # Entry-side anchors:
    # - LONG mindig ár alatti liquidity zónához igazodjon
    # - SHORT mindig ár feletti liquidity zónához igazodjon
    bull_trigger = d.get('liq_below_htf') or d.get('prev_1h_low') or d.get('liq_below_1') or d.get('prev_15m_low') or d.get('prev_5m_low')
    bear_trigger = d.get('liq_above_htf') or d.get('prev_1h_high') or d.get('liq_above_1') or d.get('prev_15m_high') or d.get('prev_5m_high')
    swing_high = d.get('swing_high_12x5m')
    swing_low = d.get('swing_low_12x5m')
    atr5 = d.get('atr_5m')
    atr15 = d.get('atr_15m')

    # Ne legyen túl közel egymáshoz a long/short belépő: 15m ATR + minimum %-os sáv.
    min_gap = None
    if last is not None:
        atr15_base = atr15 if atr15 is not None else 0.0
        min_gap = max(atr15_base * 0.95, last * 0.0048)

    # A long (bull_trigger) legyen a short (bear_trigger) alatt legalább min_gap távolsággal.
    if None not in (bull_trigger, bear_trigger, min_gap) and bear_trigger <= bull_trigger + min_gap:
        htf_up = d.get('liq_above_htf') or d.get('vwap_24h') or d.get('prev_1h_high') or d.get('prev_15m_high')
        htf_dn = d.get('liq_below_htf') or d.get('vwap_1h') or d.get('prev_1h_low') or d.get('prev_15m_low')

        if htf_dn is not None and htf_dn < bear_trigger:
            bull_trigger = min(bull_trigger, htf_dn)
        if htf_up is not None and htf_up > bull_trigger:
            bear_trigger = max(bear_trigger, htf_up)

        if None not in (bull_trigger, bear_trigger, min_gap) and bear_trigger <= bull_trigger + min_gap and last is not None:
            bull_trigger = min(bull_trigger, last - min_gap * 0.5)
            bear_trigger = max(bear_trigger, last + min_gap * 0.5)

    return {
        'bull_trigger_price': bull_trigger,
        'bear_trigger_price': bear_trigger,
        'trigger_min_separation_abs': min_gap,
        'trigger_min_separation_pct': (min_gap / last * 100.0) if None not in (min_gap, last) and last != 0 else None,
        'invalidation_long': swing_low,
        'invalidation_short': swing_high,
        'atr_stop_long': last - atr5 * 1.2 if None not in (last, atr5) else None,
        'atr_stop_short': last + atr5 * 1.2 if None not in (last, atr5) else None,
        'target_long_1': bull_trigger + atr15 * 0.8 if None not in (bull_trigger, atr15) else None,
        'target_long_2': bull_trigger + atr15 * 1.8 if None not in (bull_trigger, atr15) else None,
        'target_short_1': bear_trigger - atr15 * 0.8 if None not in (bear_trigger, atr15) else None,
        'target_short_2': bear_trigger - atr15 * 1.8 if None not in (bear_trigger, atr15) else None,
    }


def trigger_acceptance(d):
    last = d.get('last')
    long_trigger = d.get('bull_trigger_price')
    short_trigger = d.get('bear_trigger_price')
    body5 = d.get('cur5m_body_pct_of_range') or 0.0
    upper = d.get('cur5m_upper_wick_pct_of_range') or 0.0
    lower = d.get('cur5m_lower_wick_pct_of_range') or 0.0
    above = False
    below = False
    if None not in (last, long_trigger):
        above = last > long_trigger and body5 > 35 and upper < 55
    if None not in (last, short_trigger):
        below = last < short_trigger and body5 > 35 and lower < 55
    if None not in (last, long_trigger) and last != 0:
        long_dist_pct = abs(last - long_trigger) / last * 100.0
        above = long_trigger < last and long_dist_pct <= 0.80 and body5 > 35 and upper < 55
    if None not in (last, short_trigger) and last != 0:
        short_dist_pct = abs(short_trigger - last) / last * 100.0
        below = short_trigger > last and short_dist_pct <= 0.80 and body5 > 35 and lower < 55
    return {'above_long_trigger_acceptance': above, 'below_short_trigger_acceptance': below}


def retest_detector(d):
    last = d.get('last')
    bull_trigger = d.get('bull_trigger_price')
    bear_trigger = d.get('bear_trigger_price')
    body5 = d.get('cur5m_body_pct_of_range') or 0.0
    upper = d.get('cur5m_upper_wick_pct_of_range') or 0.0
    lower = d.get('cur5m_lower_wick_pct_of_range') or 0.0
    delta = d.get('recent_notional_delta_pct') or 0.0
    cvd = d.get('cvd_trend_usd') or 0.0
    p1 = d.get('price_vs_vwap_1h_pct') or 0.0
    ob = d.get('orderbook_imbalance_0_25_pct') or 0.0
    nearest = d.get('nearest_liquidity_side')
    long_score = short_score = 0.0
    if None not in (last, bull_trigger) and last != 0:
        dist = abs(last - bull_trigger) / last * 100.0
        if dist <= 0.10:
            long_score += 35
        elif dist <= 0.20:
            long_score += 20
        elif dist <= 0.35:
            long_score += 10
        if p1 > -0.05:
btc_pro_market.py
btc_pro_market.py
+58
-11

import atexit, json, re, ssl, threading, time
from collections import deque
from statistics import median
from btc_pro_config import (
    BINANCE_URL_BOOK_TICKER,
    BINANCE_URL_DEPTH,
    BINANCE_URL_KLINES,
    BINANCE_URL_TICKER_24H,
    CME_BTC_BENCHMARK_URL,
    CME_BTC_VOLUME_OI_URL,
    DERIBIT_BTC_PERP,
    DERIBIT_URL_BOOK_SUMMARY_BY_CURRENCY,
    DERIBIT_URL_BOOK_SUMMARY_BY_INSTRUMENT,
    SESSION,
    SYMBOL_PERP,
    SYMBOL_SPOT,
    URL_FUNDING,
    URL_GLOBAL,
    URL_INSTRUMENTS,
    URL_KLINE,
    URL_OI,
    URL_ORDERBOOK,
    URL_TICKERS,
    URL_TRADES,
    WS_PUBLIC,
    f,
    fmt_side,
    pct,
    safe_div,
)
from btc_pro_sources import attach_source, resolve_route
@@ -243,66 +244,111 @@ def ticker(category, symbol):
def funding_history(symbol=SYMBOL_PERP, limit=10):
    rows = req_bybit(URL_FUNDING, {'category': 'linear', 'symbol': symbol, 'limit': limit})['result']['list']
    vals = []
    for r in rows:
        rate = f(r.get('fundingRate')); vals.append(None if rate is None else rate * 100.0)
    latest = vals[0] if vals else None; prev = vals[1] if len(vals) > 1 else None
    slope = None if None in (latest, prev) else latest - prev
    accel = (vals[0] - vals[1]) - (vals[1] - vals[2]) if len(vals) > 2 and None not in (vals[0], vals[1], vals[2]) else None
    return {'funding_history_pct': vals, 'funding_slope_pct_pt': slope, 'funding_accel_pct_pt': accel, 'source_funding': 'bybit'}


def open_interest(symbol, interval):
    rows = req_bybit(URL_OI, {'category': 'linear', 'symbol': symbol, 'intervalTime': interval, 'limit': 2})['result']['list']
    vals = [f(r.get('openInterest')) for r in rows]
    now = vals[0] if vals else None; prev = vals[1] if len(vals) > 1 else None
    return now, pct(now, prev)


def all_oi(symbol=SYMBOL_PERP):
    out = {'source_open_interest': 'bybit'}
    for api_interval, key in {'5min': '5m', '15min': '15m', '30min': '30m', '1h': '1h', '4h': '4h', '1d': '1d'}.items():
        now, chg = open_interest(symbol, api_interval); out[f'oi_{key}'] = now; out[f'oi_change_{key}_pct'] = chg
    return out


def orderbook(category, symbol, limit=200):
    ob = req_bybit(URL_ORDERBOOK, {'category': category, 'symbol': symbol, 'limit': limit})['result']
    bids = [(f(x[0]), f(x[1])) for x in ob['b']]; asks = [(f(x[0]), f(x[1])) for x in ob['a']]
    bids = [(p, q) for p, q in bids if p is not None and q is not None]; asks = [(p, q) for p, q in asks if p is not None and q is not None]
    if not bids or not asks: return {}
def _book_stats(bids, asks, source_name):
    bids = [(p, q) for p, q in bids if p is not None and q is not None]
    asks = [(p, q) for p, q in asks if p is not None and q is not None]
    if not bids or not asks:
        return {}
    mid = (bids[0][0] + asks[0][0]) / 2.0

    def depth_notional(side_rows, pct_band, side):
        total = 0.0; thr = mid * (1 - pct_band / 100.0) if side == 'bid' else mid * (1 + pct_band / 100.0)
        total = 0.0
        thr = mid * (1 - pct_band / 100.0) if side == 'bid' else mid * (1 + pct_band / 100.0)
        for p, q in side_rows:
            if (side == 'bid' and p >= thr) or (side == 'ask' and p <= thr): total += p * q
            if (side == 'bid' and p >= thr) or (side == 'ask' and p <= thr):
                total += p * q
        return total

    def imbalance(pct_band):
        b = depth_notional(bids, pct_band, 'bid'); a = depth_notional(asks, pct_band, 'ask')
        if (a + b) == 0: return None
        b = depth_notional(bids, pct_band, 'bid')
        a = depth_notional(asks, pct_band, 'ask')
        if (a + b) == 0:
            return None
        return (b - a) / (b + a) * 100.0
    return {'orderbook_mid': mid, 'orderbook_imbalance_0_10_pct': imbalance(0.10), 'orderbook_imbalance_0_25_pct': imbalance(0.25), 'orderbook_imbalance_0_50_pct': imbalance(0.50), 'orderbook_imbalance_1_00_pct': imbalance(1.00), 'source_orderbook': 'bybit'}

    bid_wall_price, bid_wall_usd = max(((p, p * q) for p, q in bids[:40]), key=lambda x: x[1], default=(None, None))
    ask_wall_price, ask_wall_usd = max(((p, p * q) for p, q in asks[:40]), key=lambda x: x[1], default=(None, None))
    return {
        'orderbook_mid': mid,
        'orderbook_imbalance_0_10_pct': imbalance(0.10),
        'orderbook_imbalance_0_25_pct': imbalance(0.25),
        'orderbook_imbalance_0_50_pct': imbalance(0.50),
        'orderbook_imbalance_1_00_pct': imbalance(1.00),
        'largest_bid_wall_price': bid_wall_price,
        'largest_ask_wall_price': ask_wall_price,
        'largest_bid_wall_usd': bid_wall_usd,
        'largest_ask_wall_usd': ask_wall_usd,
        'source_orderbook': source_name,
    }


def bybit_orderbook(category, symbol, limit=200):
    ob = req_bybit(URL_ORDERBOOK, {'category': category, 'symbol': symbol, 'limit': limit})['result']
    bids = [(f(x[0]), f(x[1])) for x in ob.get('b', [])]
    asks = [(f(x[0]), f(x[1])) for x in ob.get('a', [])]
    return _book_stats(bids, asks, 'bybit')


def binance_orderbook(symbol, limit=200):
    ob = req_binance(BINANCE_URL_DEPTH, {'symbol': symbol, 'limit': min(limit, 1000)})
    bids = [(f(x[0]), f(x[1])) for x in ob.get('bids', [])]
    asks = [(f(x[0]), f(x[1])) for x in ob.get('asks', [])]
    return _book_stats(bids, asks, 'binance')


def orderbook(category, symbol, limit=200):
    routed, source = resolve_route(
        'orderbook',
        {
            'bybit': lambda: bybit_orderbook(category, symbol, limit),
            'binance': lambda: binance_orderbook(symbol, limit),
        },
    )
    return attach_source(routed, source, 'orderbook')


def recent_trades(category, symbol, limit=1000):
    rows = req_bybit(URL_TRADES, {'category': category, 'symbol': symbol, 'limit': limit})['result']['list']
    buy_count = sell_count = 0; buy_qty = sell_qty = 0.0; buy_notional = sell_notional = 0.0; large_100k_buy_usd = large_100k_sell_usd = 0.0; signed = []
    for r in rows:
        side = fmt_side(r.get('side')); price = f(r.get('price'), 0.0); size = f(r.get('size'), 0.0); notion = price * size
        if side == 'Buy':
            buy_count += 1; buy_qty += size; buy_notional += notion; signed.append(notion)
            if notion >= 100000: large_100k_buy_usd += notion
        elif side == 'Sell':
            sell_count += 1; sell_qty += size; sell_notional += notion; signed.append(-notion)
            if notion >= 100000: large_100k_sell_usd += notion
    total_count = buy_count + sell_count; total_qty = buy_qty + sell_qty; total_notional = buy_notional + sell_notional
    cvd_last = sum(signed) if signed else None; cvd_last_100 = sum(signed[-100:]) if signed else None; cvd_last_250 = sum(signed[-250:]) if signed else None
    cvd_trend = (sum(signed[len(signed)//2:]) - sum(signed[:len(signed)//2])) if len(signed) >= 2 else None
    return {'recent_trades_count': total_count, 'recent_taker_buy_ratio_pct': None if total_count == 0 else buy_count / total_count * 100.0, 'recent_buy_notional_usd': buy_notional, 'recent_sell_notional_usd': sell_notional, 'recent_notional_delta_pct': None if total_notional == 0 else (buy_notional - sell_notional) / total_notional * 100.0, 'recent_qty_delta_pct': None if total_qty == 0 else (buy_qty - sell_qty) / total_qty * 100.0, 'large_100k_buy_usd': large_100k_buy_usd, 'large_100k_sell_usd': large_100k_sell_usd, 'cvd_last_usd': cvd_last, 'cvd_last_100_usd': cvd_last_100, 'cvd_last_250_usd': cvd_last_250, 'cvd_trend_usd': cvd_trend, 'source_recent_trades': 'bybit'}


def klines(category, symbol, interval, limit):
    rows = req_bybit(URL_KLINE, {'category': category, 'symbol': symbol, 'interval': interval, 'limit': limit})['result']['list']
    return [{'ts': int(r[0]), 'open': f(r[1]), 'high': f(r[2]), 'low': f(r[3]), 'close': f(r[4]), 'volume': f(r[5]), 'turnover': f(r[6])} for r in rows]


def binance_klines(symbol, interval, limit):
@@ -332,53 +378,54 @@ def calc_atr(candles, period=14):

def candle_metrics(c):
    if not c: return {}
    o, h, l, cl = c['open'], c['high'], c['low'], c['close']
    if None in (o, h, l, cl): return {}
    rng = h - l; body = abs(cl - o); upper = h - max(o, cl); lower = min(o, cl) - l
    return {'body_pct_of_range': None if rng == 0 else body / rng * 100.0, 'upper_wick_pct_of_range': None if rng == 0 else upper / rng * 100.0, 'lower_wick_pct_of_range': None if rng == 0 else lower / rng * 100.0, 'is_green': cl >= o, 'range_abs': rng}


def calc_vwap(candles):
    pv = vol = 0.0
    for c in candles:
        if None in (c['high'], c['low'], c['close'], c['volume']): continue
        typical = (c['high'] + c['low'] + c['close']) / 3.0; pv += typical * c['volume']; vol += c['volume']
    return None if vol == 0 else pv / vol


def volume_and_structure(category, symbol):
    k5 = klines(category, symbol, '5', 80); k15 = klines(category, symbol, '15', 50); k60 = klines(category, symbol, '60', 30)
    cur5 = k5[0] if k5 else None; cur15 = k15[0] if k15 else None; cur60 = k60[0] if k60 else None
    t5 = cur5['turnover'] if cur5 else None; t15 = cur15['turnover'] if cur15 else None; t60 = cur60['turnover'] if cur60 else None
    med5 = median_turnover_ex_current(k5, 20); med15 = median_turnover_ex_current(k15, 20); med60 = median_turnover_ex_current(k60, 20)
    medr5 = median_range_ex_current(k5, 20); medr15 = median_range_ex_current(k15, 20)
    cur5r = None if not cur5 else (cur5['high'] - cur5['low']); cur15r = None if not cur15 else (cur15['high'] - cur15['low'])
    prev5_high = k5[1]['high'] if len(k5) > 1 else None; prev5_low = k5[1]['low'] if len(k5) > 1 else None; prev15_high = k15[1]['high'] if len(k15) > 1 else None; prev15_low = k15[1]['low'] if len(k15) > 1 else None
    prev1h_high = k60[1]['high'] if len(k60) > 1 else None; prev1h_low = k60[1]['low'] if len(k60) > 1 else None
    highs = [c['high'] for c in k5[:12] if c['high'] is not None]; lows = [c['low'] for c in k5[:12] if c['low'] is not None]
    vwap1 = calc_vwap(k5[:12]); vwap24 = calc_vwap(k60[:24]); last = cur5['close'] if cur5 else None
    out = {'turnover_5m_usd': t5, 'turnover_15m_usd': t15, 'turnover_1h_usd': t60, 'median_turnover_5m_usd': med5, 'median_turnover_15m_usd': med15, 'median_turnover_1h_usd': med60, 'volume_spike_5m_x': safe_div(t5, med5), 'volume_spike_15m_x': safe_div(t15, med15), 'volume_spike_1h_x': safe_div(t60, med60), 'range_5m': cur5r, 'range_15m': cur15r, 'median_range_5m': medr5, 'median_range_15m': medr15, 'range_expansion_5m_x': safe_div(cur5r, medr5), 'range_expansion_15m_x': safe_div(cur15r, medr15), 'prev_5m_high': prev5_high, 'prev_5m_low': prev5_low, 'prev_15m_high': prev15_high, 'prev_15m_low': prev15_low, 'swing_high_12x5m': max(highs) if highs else None, 'swing_low_12x5m': min(lows) if lows else None, 'vwap_1h': vwap1, 'vwap_24h': vwap24, 'price_vs_vwap_1h_pct': pct(last, vwap1), 'price_vs_vwap_24h_pct': pct(last, vwap24), 'atr_5m': calc_atr(k5, 14), 'atr_15m': calc_atr(k15, 14), 'price_low_2_5m': min([c['low'] for c in k5[:2] if c['low'] is not None], default=None), 'price_low_6_5m': min([c['low'] for c in k5[1:7] if c['low'] is not None], default=None), 'price_high_2_5m': max([c['high'] for c in k5[:2] if c['high'] is not None], default=None), 'price_high_6_5m': max([c['high'] for c in k5[1:7] if c['high'] is not None], default=None), 'vol_declining_5m': True if len(k5) > 2 and all(k5[i]['turnover'] <= k5[i+1]['turnover'] for i in range(0, 2) if None not in (k5[i]['turnover'], k5[i+1]['turnover'])) else False, 'source_structure': 'bybit'}
    out = {'turnover_5m_usd': t5, 'turnover_15m_usd': t15, 'turnover_1h_usd': t60, 'median_turnover_5m_usd': med5, 'median_turnover_15m_usd': med15, 'median_turnover_1h_usd': med60, 'volume_spike_5m_x': safe_div(t5, med5), 'volume_spike_15m_x': safe_div(t15, med15), 'volume_spike_1h_x': safe_div(t60, med60), 'range_5m': cur5r, 'range_15m': cur15r, 'median_range_5m': medr5, 'median_range_15m': medr15, 'range_expansion_5m_x': safe_div(cur5r, medr5), 'range_expansion_15m_x': safe_div(cur15r, medr15), 'prev_5m_high': prev5_high, 'prev_5m_low': prev5_low, 'prev_15m_high': prev15_high, 'prev_15m_low': prev15_low, 'prev_1h_high': prev1h_high, 'prev_1h_low': prev1h_low, 'swing_high_12x5m': max(highs) if highs else None, 'swing_low_12x5m': min(lows) if lows else None, 'vwap_1h': vwap1, 'vwap_24h': vwap24, 'price_vs_vwap_1h_pct': pct(last, vwap1), 'price_vs_vwap_24h_pct': pct(last, vwap24), 'atr_5m': calc_atr(k5, 14), 'atr_15m': calc_atr(k15, 14), 'price_low_2_5m': min([c['low'] for c in k5[:2] if c['low'] is not None], default=None), 'price_low_6_5m': min([c['low'] for c in k5[1:7] if c['low'] is not None], default=None), 'price_high_2_5m': max([c['high'] for c in k5[:2] if c['high'] is not None], default=None), 'price_high_6_5m': max([c['high'] for c in k5[1:7] if c['high'] is not None], default=None), 'vol_declining_5m': True if len(k5) > 2 and all(k5[i]['turnover'] <= k5[i+1]['turnover'] for i in range(0, 2) if None not in (k5[i]['turnover'], k5[i+1]['turnover'])) else False, 'source_structure': 'bybit'}
    out.update({f'cur5m_{k}': v for k, v in candle_metrics(cur5).items()}); out.update({f'cur15m_{k}': v for k, v in candle_metrics(cur15).items()})
    return out


def spot_perp_divergence():
    perp = ticker('linear', SYMBOL_PERP)
    spot = ticker('spot', SYMBOL_SPOT)
    perp_k5 = klines('linear', SYMBOL_PERP, '5', 2)

    routed_spot_klines, spot_kline_source = resolve_route(
        'spot_klines',
        {
            'binance': lambda: binance_klines(SYMBOL_SPOT, '5m', 2),
            'bybit': lambda: klines('spot', SYMBOL_SPOT, '5', 2),
        },
    )
    spot_k5 = routed_spot_klines if isinstance(routed_spot_klines, list) else []

    perp_chg = pct(perp_k5[0]['close'], perp_k5[1]['close']) if len(perp_k5) > 1 else None
    spot_chg = pct(spot_k5[0]['close'], spot_k5[1]['close']) if len(spot_k5) > 1 else None
    return {
        'spot_last': spot.get('last'),
        'spot_chg24_pct': spot.get('chg24_pct'),
        'spot_turnover24_usd': spot.get('turnover24_usd'),
        'spot_volume24_btc': spot.get('volume24'),