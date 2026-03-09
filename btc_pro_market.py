# btc_pro_market.py
import time
import math
import statistics
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

SYMBOL = "BTCUSDT"

URL_BYBIT_BASE = "https://api.bybit.com"
URL_INSTRUMENTS = f"{URL_BYBIT_BASE}/v5/market/instruments-info"
URL_TICKERS = f"{URL_BYBIT_BASE}/v5/market/tickers"
URL_ORDERBOOK = f"{URL_BYBIT_BASE}/v5/market/orderbook"
URL_KLINE = f"{URL_BYBIT_BASE}/v5/market/kline"
URL_RECENT_TRADES = f"{URL_BYBIT_BASE}/v5/market/recent-trade"
URL_OPEN_INTEREST = f"{URL_BYBIT_BASE}/v5/market/open-interest"
URL_FUNDING_HISTORY = f"{URL_BYBIT_BASE}/v5/market/funding/history"

URL_BINANCE_BASE = "https://api.binance.com"
URL_BINANCE_TICKER_24H = f"{URL_BINANCE_BASE}/api/v3/ticker/24hr"
URL_BINANCE_TICKER_PRICE = f"{URL_BINANCE_BASE}/api/v3/ticker/price"
URL_BINANCE_KLINES = f"{URL_BINANCE_BASE}/api/v3/klines"

URL_GLOBAL = "https://api.coingecko.com/api/v3/global"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
})


def now_bucharest_str():
    return datetime.now(ZoneInfo("Europe/Bucharest")).strftime("%Y-%m-%d %H:%M:%S")


def _to_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _to_int(x, default=0):
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def _median(values, default=0.0):
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return default
    return statistics.median(vals)


def _pct_change(new, old, default=0.0):
    new = _to_float(new, None)
    old = _to_float(old, None)
    if new is None or old in (None, 0):
        return default
    return ((new / old) - 1.0) * 100.0


def _safe_div(a, b, default=0.0):
    a = _to_float(a, None)
    b = _to_float(b, None)
    if a is None or b in (None, 0):
        return default
    return a / b


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def req(url, params=None, timeout=12):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def req_bybit(url, params=None, timeout=12):
    return req(url, params=params, timeout=timeout)


def req_binance(url, params=None, timeout=12):
    return req(url, params=params, timeout=timeout)


def instrument_info(symbol=SYMBOL):
    items = req_bybit(URL_INSTRUMENTS, {"category": "linear", "symbol": symbol})["result"]["list"]
    item = items[0] if items else {}
    return {
        "funding_interval_min": _to_int(item.get("fundingInterval"), 480),
        "launch_time_ms": str(item.get("launchTime", "")),
        "price_scale": str(item.get("priceScale", "")),
        "source_instrument_info": "bybit",
    }


def bybit_ticker(symbol=SYMBOL):
    items = req_bybit(URL_TICKERS, {"category": "linear", "symbol": symbol})["result"]["list"]
    item = items[0] if items else {}
    return {
        "last": _to_float(item.get("lastPrice")),
        "bid1": _to_float(item.get("bid1Price")),
        "ask1": _to_float(item.get("ask1Price")),
        "low24": _to_float(item.get("lowPrice24h")),
        "high24": _to_float(item.get("highPrice24h")),
        "prev24": _to_float(item.get("prevPrice24h")),
        "chg24_pct": _to_float(item.get("price24hPcnt")) * 100.0,
        "turnover24_usd": _to_float(item.get("turnover24h")),
        "volume24": _to_float(item.get("volume24h")),
        "mark": _to_float(item.get("markPrice")),
        "index": _to_float(item.get("indexPrice")),
        "open_interest": _to_float(item.get("openInterest")),
        "funding_pct": _to_float(item.get("fundingRate")) * 100.0,
        "source_ticker": "bybit",
    }


def bybit_funding_history(symbol=SYMBOL, limit=10):
    data = req_bybit(
        URL_FUNDING_HISTORY,
        {"category": "linear", "symbol": symbol, "limit": limit},
    )["result"]["list"]
    vals = [_to_float(x.get("fundingRate")) * 100.0 for x in data]
    vals = list(reversed(vals))
    slope = 0.0
    accel = 0.0
    if len(vals) >= 2:
        slope = vals[-1] - vals[-2]
    if len(vals) >= 3:
        accel = (vals[-1] - vals[-2]) - (vals[-2] - vals[-3])
    return {
        "funding_history_pct": vals,
        "funding_slope_pct_pt": slope,
        "funding_accel_pct_pt": accel,
        "source_funding": "bybit",
    }


def bybit_orderbook(symbol=SYMBOL, limit=50):
    data = req_bybit(
        URL_ORDERBOOK,
        {"category": "linear", "symbol": symbol, "limit": limit},
    )["result"]

    bids = data.get("b", []) or []
    asks = data.get("a", []) or []

    bid_rows = [(_to_float(p), _to_float(q)) for p, q in bids]
    ask_rows = [(_to_float(p), _to_float(q)) for p, q in asks]

    best_bid = bid_rows[0][0] if bid_rows else 0.0
    best_ask = ask_rows[0][0] if ask_rows else 0.0
    mid = (best_bid + best_ask) / 2.0 if best_bid and best_ask else 0.0

    def side_notional(rows, pct):
        if not mid:
            return 0.0
        lo = mid * (1.0 - pct)
        hi = mid * (1.0 + pct)
        total = 0.0
        for price, qty in rows:
            if lo <= price <= hi:
                total += price * qty
        return total

    bands = [0.001, 0.0025, 0.005, 0.01]
    out = {
        "orderbook_mid": mid,
        "source_orderbook": "bybit",
    }

    for pct in bands:
        bid_notional = side_notional(bid_rows, pct)
        ask_notional = side_notional(ask_rows, pct)
        total = bid_notional + ask_notional
        imbalance = 0.0 if total == 0 else ((bid_notional - ask_notional) / total) * 100.0
        key = f"{pct:.2%}".replace("%", "").replace(".", "_")
        out[f"orderbook_imbalance_{key}_pct"] = imbalance

    if bid_rows:
        largest_bid = max(bid_rows, key=lambda x: x[0] * x[1])
        out["largest_bid_wall_price"] = largest_bid[0]
        out["largest_bid_wall_usd"] = largest_bid[0] * largest_bid[1]
    else:
        out["largest_bid_wall_price"] = 0.0
        out["largest_bid_wall_usd"] = 0.0

    if ask_rows:
        largest_ask = max(ask_rows, key=lambda x: x[0] * x[1])
        out["largest_ask_wall_price"] = largest_ask[0]
        out["largest_ask_wall_usd"] = largest_ask[0] * largest_ask[1]
    else:
        out["largest_ask_wall_price"] = 0.0
        out["largest_ask_wall_usd"] = 0.0

    if out["largest_bid_wall_usd"] > out["largest_ask_wall_usd"]:
        out["wall_pressure_side"] = "bid"
    elif out["largest_ask_wall_usd"] > out["largest_bid_wall_usd"]:
        out["wall_pressure_side"] = "ask"
    else:
        out["wall_pressure_side"] = "neutral"

    return out


def bybit_recent_trades(symbol=SYMBOL, limit=1000):
    items = req_bybit(
        URL_RECENT_TRADES,
        {"category": "linear", "symbol": symbol, "limit": limit},
    )["result"]["list"]

    buy_notional = 0.0
    sell_notional = 0.0
    buy_qty = 0.0
    sell_qty = 0.0
    large_buy = 0.0
    large_sell = 0.0
    signed = []

    for row in items:
        price = _to_float(row.get("price"))
        qty = _to_float(row.get("size"))
        side = str(row.get("side", "")).lower()
        notional = price * qty
        if side == "buy":
            buy_notional += notional
            buy_qty += qty
            signed.append(notional)
            if notional >= 100000:
                large_buy += notional
        else:
            sell_notional += notional
            sell_qty += qty
            signed.append(-notional)
            if notional >= 100000:
                large_sell += notional

    total_notional = buy_notional + sell_notional
    total_qty = buy_qty + sell_qty
    taker_buy_ratio = 0.0 if total_qty == 0 else (buy_qty / total_qty) * 100.0
    delta_pct = 0.0 if total_notional == 0 else ((buy_notional - sell_notional) / total_notional) * 100.0
    qty_delta_pct = 0.0 if total_qty == 0 else ((buy_qty - sell_qty) / total_qty) * 100.0

    cvd_last = sum(signed)
    cvd_last_100 = sum(signed[-100:]) if len(signed) >= 100 else cvd_last
    cvd_last_250 = sum(signed[-250:]) if len(signed) >= 250 else cvd_last
    cvd_trend = cvd_last_100 - cvd_last_250

    return {
        "recent_trades_count": len(items),
        "recent_taker_buy_ratio_pct": taker_buy_ratio,
        "recent_buy_notional_usd": buy_notional,
        "recent_sell_notional_usd": sell_notional,
        "recent_notional_delta_pct": delta_pct,
        "recent_qty_delta_pct": qty_delta_pct,
        "large_100k_buy_usd": large_buy,
        "large_100k_sell_usd": large_sell,
        "cvd_last_usd": cvd_last,
        "cvd_last_100_usd": cvd_last_100,
        "cvd_last_250_usd": cvd_last_250,
        "cvd_trend_usd": cvd_trend,
        "source_recent_trades": "bybit",
    }


def _klines_bybit(symbol=SYMBOL, interval="5", limit=60):
    data = req_bybit(
        URL_KLINE,
        {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit},
    )["result"]["list"]

    rows = []
    for row in reversed(data):
        ts, o, h, l, c, v, t = row[:7]
        rows.append({
            "ts": _to_int(ts),
            "open": _to_float(o),
            "high": _to_float(h),
            "low": _to_float(l),
            "close": _to_float(c),
            "volume": _to_float(v),
            "turnover": _to_float(t),
        })
    return rows


def _atr(rows, period=14):
    if len(rows) < 2:
        return 0.0
    trs = []
    prev_close = rows[0]["close"]
    for r in rows[1:]:
        tr = max(
            r["high"] - r["low"],
            abs(r["high"] - prev_close),
            abs(r["low"] - prev_close),
        )
        trs.append(tr)
        prev_close = r["close"]
    if not trs:
        return 0.0
    return _median(trs[-period:], default=0.0)


def _vwap(rows):
    num = 0.0
    den = 0.0
    for r in rows:
        typical = (r["high"] + r["low"] + r["close"]) / 3.0
        vol = r["volume"]
        num += typical * vol
        den += vol
    return 0.0 if den == 0 else num / den


def _trend_label(closes):
    if len(closes) < 4:
        return "range"
    last = closes[-1]
    sma_short = sum(closes[-3:]) / 3.0
    sma_long = sum(closes[-6:]) / 6.0 if len(closes) >= 6 else sum(closes) / len(closes)
    if last > sma_short > sma_long:
        return "up"
    if last < sma_short < sma_long:
        return "down"
    return "range"


def bybit_structure(symbol=SYMBOL):
    k5 = _klines_bybit(symbol, "5", 60)
    k15 = _klines_bybit(symbol, "15", 60)
    k60 = _klines_bybit(symbol, "60", 60)

    closes_5 = [x["close"] for x in k5]
    closes_15 = [x["close"] for x in k15]
    closes_60 = [x["close"] for x in k60]

    last5 = k5[-1] if k5 else {}
    last15 = k15[-1] if k15 else {}
    prev5 = k5[-2] if len(k5) >= 2 else {}
    prev15 = k15[-2] if len(k15) >= 2 else {}

    med_turnover_5 = _median([x["turnover"] for x in k5[-20:-1]], 0.0)
    med_turnover_15 = _median([x["turnover"] for x in k15[-20:-1]], 0.0)
    med_turnover_1h = _median([x["turnover"] for x in k60[-20:-1]], 0.0)

    med_range_5 = _median([(x["high"] - x["low"]) for x in k5[-20:-1]], 0.0)
    med_range_15 = _median([(x["high"] - x["low"]) for x in k15[-20:-1]], 0.0)

    range_5 = _to_float(last5.get("high")) - _to_float(last5.get("low"))
    range_15 = _to_float(last15.get("high")) - _to_float(last15.get("low"))

    atr5 = _atr(k5, 14)
    atr15 = _atr(k15, 14)
    vwap_1h = _vwap(k5[-12:]) if len(k5) >= 12 else _vwap(k5)
    vwap_24h = _vwap(k60[-24:]) if len(k60) >= 24 else _vwap(k60)

    cur5_body = abs(_to_float(last5.get("close")) - _to_float(last5.get("open")))
    cur15_body = abs(_to_float(last15.get("close")) - _to_float(last15.get("open")))

    def candle_stats(row):
        o = _to_float(row.get("open"))
        h = _to_float(row.get("high"))
        l = _to_float(row.get("low"))
        c = _to_float(row.get("close"))
        r = h - l
        if r <= 0:
            return 0.0, 0.0, 0.0, False, 0.0
        body = abs(c - o)
        upper = h - max(o, c)
        lower = min(o, c) - l
        return (body / r) * 100.0, (upper / r) * 100.0, (lower / r) * 100.0, c >= o, r

    c5_body, c5_up, c5_low, c5_green, c5_range = candle_stats(last5)
    c15_body, c15_up, c15_low, c15_green, c15_range = candle_stats(last15)

    highs12 = [x["high"] for x in k5[-12:]] if len(k5) >= 12 else [x["high"] for x in k5]
    lows12 = [x["low"] for x in k5[-12:]] if len(k5) >= 12 else [x["low"] for x in k5]

    turns_5 = [x["turnover"] for x in k5[-12:]]
    turns_15 = [x["turnover"] for x in k15[-12:]]
    turns_1h = [x["turnover"] for x in k60[-12:]]

    out = {
        "turnover_5m_usd": _to_float(last5.get("turnover")),
        "turnover_15m_usd": _to_float(last15.get("turnover")),
        "turnover_1h_usd": _to_float(k60[-1]["turnover"]) if k60 else 0.0,
        "median_turnover_5m_usd": med_turnover_5,
        "median_turnover_15m_usd": med_turnover_15,
        "median_turnover_1h_usd": med_turnover_1h,
        "volume_spike_5m_x": _safe_div(_to_float(last5.get("turnover")), med_turnover_5, 0.0),
        "volume_spike_15m_x": _safe_div(_to_float(last15.get("turnover")), med_turnover_15, 0.0),
        "volume_spike_1h_x": _safe_div(_to_float(k60[-1]["turnover"]) if k60 else 0.0, med_turnover_1h, 0.0),
        "range_5m": range_5,
        "range_15m": range_15,
        "median_range_5m": med_range_5,
        "median_range_15m": med_range_15,
        "range_expansion_5m_x": _safe_div(range_5, med_range_5, 0.0),
        "range_expansion_15m_x": _safe_div(range_15, med_range_15, 0.0),
        "prev_5m_high": _to_float(prev5.get("high")),
        "prev_5m_low": _to_float(prev5.get("low")),
        "prev_15m_high": _to_float(prev15.get("high")),
        "prev_15m_low": _to_float(prev15.get("low")),
        "swing_high_12x5m": max(highs12) if highs12 else 0.0,
        "swing_low_12x5m": min(lows12) if lows12 else 0.0,
        "vwap_1h": vwap_1h,
        "vwap_24h": vwap_24h,
        "price_vs_vwap_1h_pct": _pct_change(closes_5[-1], vwap_1h, 0.0),
        "price_vs_vwap_24h_pct": _pct_change(closes_5[-1], vwap_24h, 0.0),
        "atr_5m": atr5,
        "atr_15m": atr15,
        "price_low_2_5m": min([x["low"] for x in k5[-2:]]) if len(k5) >= 2 else 0.0,
        "price_low_6_5m": min([x["low"] for x in k5[-6:]]) if len(k5) >= 6 else 0.0,
        "price_high_2_5m": max([x["high"] for x in k5[-2:]]) if len(k5) >= 2 else 0.0,
        "price_high_6_5m": max([x["high"] for x in k5[-6:]]) if len(k5) >= 6 else 0.0,
        "vol_declining_5m": len(turns_5) >= 3 and turns_5[-1] < turns_5[-2] < turns_5[-3],
        "source_structure": "bybit",
        "cur5m_body_pct_of_range": c5_body,
        "cur5m_upper_wick_pct_of_range": c5_up,
        "cur5m_lower_wick_pct_of_range": c5_low,
        "cur5m_is_green": c5_green,
        "cur5m_range_abs": c5_range,
        "cur15m_body_pct_of_range": c15_body,
        "cur15m_upper_wick_pct_of_range": c15_up,
        "cur15m_lower_wick_pct_of_range": c15_low,
        "cur15m_is_green": c15_green,
        "cur15m_range_abs": c15_range,
        "trend_5m": _trend_label(closes_5),
        "trend_15m": _trend_label(closes_15),
        "trend_1h": _trend_label(closes_60),
    }
    return out


def bybit_open_interest_context(symbol=SYMBOL):
    current_oi = bybit_ticker(symbol).get("open_interest", 0.0)

    out = {
        "source_open_interest": "bybit",
        "oi_5m": current_oi,
        "oi_15m": current_oi,
        "oi_30m": current_oi,
        "oi_1h": current_oi,
        "oi_4h": current_oi,
        "oi_1d": current_oi,
        "oi_change_5m_pct": 0.0,
        "oi_change_15m_pct": 0.0,
        "oi_change_30m_pct": 0.0,
        "oi_change_1h_pct": 0.0,
        "oi_change_4h_pct": 0.0,
        "oi_change_1d_pct": 0.0,
    }
    return out


def binance_spot_context(symbol=SYMBOL):
    t24 = req_binance(URL_BINANCE_TICKER_24H, {"symbol": symbol})
    spot_last_row = req_binance(URL_BINANCE_TICKER_PRICE, {"symbol": symbol})
    kl5 = req_binance(URL_BINANCE_KLINES, {"symbol": symbol, "interval": "5m", "limit": 3})

    spot_last = _to_float(spot_last_row.get("price"))
    spot_chg24_pct = _to_float(t24.get("priceChangePercent"))
    spot_turnover24 = _to_float(t24.get("quoteVolume"))
    spot_volume24 = _to_float(t24.get("volume"))

    close_prev = _to_float(kl5[-2][4]) if len(kl5) >= 2 else spot_last
    close_last = _to_float(kl5[-1][4]) if len(kl5) >= 1 else spot_last
    spot_5m_chg = _pct_change(close_last, close_prev, 0.0)

    return {
        "spot_last": spot_last,
        "spot_chg24_pct": spot_chg24_pct,
        "spot_turnover24_usd": spot_turnover24,
        "spot_volume24_btc": spot_volume24,
        "spot_5m_chg_pct": spot_5m_chg,
        "source_spot_context": "binance",
        "source_spot_klines": "binance",
    }


def global_context():
    data = req(URL_GLOBAL)
    d = data.get("data", {})
    return {
        "total_mcap_usd": _to_float(d.get("total_market_cap", {}).get("usd")),
        "btc_dom_pct": _to_float(d.get("market_cap_percentage", {}).get("btc")),
    }


def merge_market_snapshot(symbol=SYMBOL):
    instrument = instrument_info(symbol)
    ticker = bybit_ticker(symbol)
    funding = bybit_funding_history(symbol)
    oi = bybit_open_interest_context(symbol)
    ob = bybit_orderbook(symbol)
    trades = bybit_recent_trades(symbol)
    structure = bybit_structure(symbol)
    spot = binance_spot_context(symbol)
    glob = global_context()

    btc = {}
    for block in [ticker, funding, oi, ob, trades, structure, spot]:
        btc.update(block)

    last = _to_float(btc.get("last"))
    low24 = _to_float(btc.get("low24"))
    high24 = _to_float(btc.get("high24"))
    index = _to_float(btc.get("index"))
    bid1 = _to_float(btc.get("bid1"))
    ask1 = _to_float(btc.get("ask1"))
    spot_last = _to_float(btc.get("spot_last"))
    bull_trigger = btc.get("bull_trigger_price", btc.get("prev_5m_high", 0.0))
    bear_trigger = btc.get("bear_trigger_price", btc.get("prev_5m_low", 0.0))
    atr5 = _to_float(btc.get("atr_5m"))

    btc["spread_bps"] = 0.0 if last == 0 else ((ask1 - bid1) / last) * 10000.0
    btc["premium_vs_index_pct"] = _pct_change(last, index, 0.0)
    btc["range_pos_pct"] = 0.0 if high24 == low24 else ((last - low24) / (high24 - low24)) * 100.0
    btc["perp_spot_last_spread_pct"] = _pct_change(last, spot_last, 0.0)
    btc["perp_5m_chg_pct"] = _pct_change(last, _to_float(btc.get("price_low_2_5m"), last), 0.0)
    btc["spot_perp_divergence_5m_pct_pt"] = _to_float(btc.get("perp_5m_chg_pct")) - _to_float(btc.get("spot_5m_chg_pct"))
    btc["source_perp_context"] = "bybit"

    btc["liq_capture_window_sec"] = 8
    btc["liq_available"] = False
    btc["long_liq_usd_5m"] = None
    btc["short_liq_usd_5m"] = None
    btc["liq_imbalance_pct"] = None

    hour_local = datetime.now(ZoneInfo("Europe/Bucharest")).hour
    if 8 <= hour_local < 17:
        session = "europe"
        liq_profile = "medium_high"
    elif 17 <= hour_local < 24:
        session = "us"
        liq_profile = "high"
    else:
        session = "asia"
        liq_profile = "medium"
    btc["session_name"] = session
    btc["session_liquidity_profile"] = liq_profile

    chop_components = [
        100.0 - abs(_to_float(btc.get("orderbook_imbalance_0_10_pct"))),
        100.0 - abs(_to_float(btc.get("recent_notional_delta_pct"))),
        100.0 - abs(_to_float(btc.get("price_vs_vwap_1h_pct")) * 25.0),
    ]
    btc["chop_score"] = _clamp(sum(chop_components) / 300.0 * 2.0, 0.0, 2.0)
    btc["multi_tf_alignment_score"] = int(btc["trend_5m"] == btc["trend_15m"]) + int(btc["trend_15m"] == btc["trend_1h"])

    btc["bull_divergence"] = False
    btc["bear_divergence"] = False
    btc["divergence_notes"] = []
    btc["absorption_buying"] = False
    btc["absorption_selling"] = False
    btc["absorption_reason"] = []

    delta_pct = _to_float(btc.get("recent_notional_delta_pct"))
    btc["delta_strength_score"] = _clamp(delta_pct * 1.2, -100.0, 100.0)
    btc["volume_quality_score"] = _clamp(
        ((_to_float(btc.get("volume_spike_5m_x")) * 35.0) + (_to_float(btc.get("volume_spike_15m_x")) * 35.0)),
        0.0,
        100.0,
    )

    liq_levels = [x for x in [btc.get("prev_5m_high"), btc.get("prev_15m_high"), btc.get("prev_5m_low"), btc.get("prev_15m_low")] if x is not None]
    above = [x for x in liq_levels if x > last]
    below = [x for x in liq_levels if x < last]
    btc["liq_above_1"] = min(above) if above else None
    btc["liq_above_2"] = sorted(above)[1] if len(above) > 1 else None
    btc["liq_below_1"] = max(below) if below else None
    btc["liq_below_2"] = sorted(below, reverse=True)[1] if len(below) > 1 else None
    if btc["liq_above_1"] is None and btc["liq_below_1"] is None:
        btc["nearest_liquidity_side"] = "none"
    elif btc["liq_above_1"] is None:
        btc["nearest_liquidity_side"] = "below"
    elif btc["liq_below_1"] is None:
        btc["nearest_liquidity_side"] = "above"
    else:
        btc["nearest_liquidity_side"] = "above" if abs(btc["liq_above_1"] - last) < abs(last - btc["liq_below_1"]) else "below"

    btc["bull_trigger_price"] = bull_trigger
    btc["bear_trigger_price"] = bear_trigger
    btc["invalidation_long"] = _to_float(btc.get("swing_low_12x5m"))
    btc["invalidation_short"] = _to_float(btc.get("swing_high_12x5m"))
    btc["atr_stop_long"] = max(0.0, last - atr5)
    btc["atr_stop_short"] = last + atr5
    btc["target_long_1"] = bull_trigger + atr5
    btc["target_long_2"] = bull_trigger + (atr5 * 2.5)
    btc["target_short_1"] = bear_trigger - atr5
    btc["target_short_2"] = bear_trigger - (atr5 * 2.5)

    btc["above_long_trigger_acceptance"] = last > bull_trigger
    btc["below_short_trigger_acceptance"] = last < bear_trigger

    long_retest = 0.0
    short_retest = 0.0
    if bull_trigger > 0:
        long_retest = _clamp(100.0 - abs((last - bull_trigger) / bull_trigger) * 10000.0, 0.0, 100.0)
    if bear_trigger > 0:
        short_retest = _clamp(100.0 - abs((last - bear_trigger) / bear_trigger) * 10000.0, 0.0, 100.0)
    if delta_pct > 0:
        long_retest += 10
    if delta_pct < 0:
        short_retest += 10
    btc["retest_long_score"] = _clamp(long_retest, 0.0, 100.0)
    btc["retest_short_score"] = _clamp(short_retest, 0.0, 100.0)
    if btc["retest_long_score"] > btc["retest_short_score"]:
        btc["retest_winner_side"] = "long"
    elif btc["retest_short_score"] > btc["retest_long_score"]:
        btc["retest_winner_side"] = "short"
    else:
        btc["retest_winner_side"] = "none"
    btc["retest_long_ready"] = btc["retest_long_score"] >= 50
    btc["retest_short_ready"] = btc["retest_short_score"] >= 50
    btc["retest_quality_score"] = max(btc["retest_long_score"], btc["retest_short_score"])
    btc["retest_score_margin"] = abs(btc["retest_long_score"] - btc["retest_short_score"])

    if btc["trend_1h"] == "up" and btc["trend_5m"] == "up":
        btc["market_regime"] = "trend_build_long"
    elif btc["trend_1h"] == "down" and btc["trend_5m"] == "down":
        btc["market_regime"] = "trend_build_short"
    elif btc["trend_1h"] == "up" and btc["trend_5m"] == "down":
        btc["market_regime"] = "short_squeeze"
    elif btc["trend_1h"] == "down" and btc["trend_5m"] == "up":
        btc["market_regime"] = "long_squeeze"
    else:
        btc["market_regime"] = "neutral"

    btc["reversal_probability_long"] = 0
    btc["reversal_probability_short"] = 0
    btc["breakout_quality_score"] = _clamp(
        (_to_float(btc.get("volume_spike_15m_x")) * 25.0)
        + (_to_float(btc.get("range_expansion_15m_x")) * 20.0)
        + (abs(delta_pct) * 0.25),
        0.0,
        100.0,
    )

    if delta_pct > 10 and last > _to_float(btc.get("vwap_1h")):
        btc["breakout_direction"] = "up"
    elif delta_pct < -10 and last < _to_float(btc.get("vwap_1h")):
        btc["breakout_direction"] = "down"
    else:
        btc["breakout_direction"] = "neutral"

    btc["bull_break_valid"] = btc["above_long_trigger_acceptance"] and btc["breakout_direction"] == "up"
    btc["bear_break_valid"] = btc["below_short_trigger_acceptance"] and btc["breakout_direction"] == "down"
    btc["reclaim_strength_score"] = 10.0 if abs(delta_pct) > 10 else 5.0

    btc["rr_long_to_t1"] = _safe_div((btc["target_long_1"] - last), (last - btc["atr_stop_long"]), 0.0)
    btc["rr_long_to_t2"] = _safe_div((btc["target_long_2"] - last), (last - btc["atr_stop_long"]), 0.0)
    btc["rr_short_to_t1"] = _safe_div((last - btc["target_short_1"]), (btc["atr_stop_short"] - last), 0.0)
    btc["rr_short_to_t2"] = _safe_div((last - btc["target_short_2"]), (btc["atr_stop_short"] - last), 0.0)
    btc["stop_quality"] = "good"

    btc["entry_quality_long"] = 0
    btc["entry_quality_short"] = 0
    btc["bull_trap_risk"] = 0
    btc["bear_trap_risk"] = 0
    btc["trap_alert"] = "none"
    btc["move_exhaustion_up"] = 15.0 if btc["range_pos_pct"] > 85 else 0.0
    btc["move_exhaustion_down"] = 15.0 if btc["range_pos_pct"] < 15 else 0.0
    btc["long_rejection_detected"] = bool(c5_up > 45 and c5_green)
    btc["short_rejection_detected"] = bool(c5_low > 45 and not c5_green)

    orderflow_consistency = 50.0
    if delta_pct > 10 and btc["trend_5m"] == "up":
        orderflow_consistency = 81.25
        side = "bullish"
    elif delta_pct < -10 and btc["trend_5m"] == "down":
        orderflow_consistency = 81.25
        side = "bearish"
    elif delta_pct > 0:
        orderflow_consistency = 68.75
        side = "bullish"
    elif delta_pct < 0:
        orderflow_consistency = 18.75
        side = "bearish"
    else:
        side = "neutral"
    btc["orderflow_consistency_score"] = orderflow_consistency
    btc["orderflow_consistency_side"] = side

    bull_score = 0.0
    bear_score = 0.0
    if btc["trend_1h"] == "up":
        bull_score += 15
    if btc["trend_5m"] == "up":
        bull_score += 10
    if delta_pct > 0:
        bull_score += min(20, delta_pct * 0.25)
    if _to_float(btc.get("orderbook_imbalance_0_10_pct")) > 0:
        bull_score += min(10, _to_float(btc.get("orderbook_imbalance_0_10_pct")) * 0.5)

    if btc["trend_1h"] == "down":
        bear_score += 15
    if btc["trend_5m"] == "down":
        bear_score += 10
    if delta_pct < 0:
        bear_score += min(20, abs(delta_pct) * 0.25)
    if _to_float(btc.get("orderbook_imbalance_0_10_pct")) < 0:
        bear_score += min(10, abs(_to_float(btc.get("orderbook_imbalance_0_10_pct"))) * 0.5)

    bull_score = _clamp(bull_score, 0.0, 100.0)
    bear_score = _clamp(bear_score, 0.0, 100.0)
    btc["bull_score"] = bull_score
    btc["bear_score"] = bear_score
    btc["confidence_score"] = _clamp(
        max(bull_score, bear_score) + (_to_float(btc.get("breakout_quality_score")) * 0.3),
        0.0,
        100.0,
    )

    raw_bias = "no_trade"
    if bull_score > bear_score + 5:
        raw_bias = "long"
    elif bear_score > bull_score + 5:
        raw_bias = "short"
    btc["raw_trade_bias"] = raw_bias
    btc["trade_bias"] = raw_bias

    return {
        "ts_bucharest": now_bucharest_str(),
        "weekend_flag": datetime.now(ZoneInfo("Europe/Bucharest")).weekday() >= 5,
        "instrument": instrument,
        "btc": btc,
        "global": glob,
        "data_quality_score": 90,
        "missing_modules": [],
        "ws_liq_status": "missing_or_inactive",
        "history_rows_loaded": 0,
        "prev_trade_bias": None,
        "prev_market_regime": None,
        "prev_breakout_quality_score": None,
        "prev_bull_score": None,
        "prev_bear_score": None,
        "bull_score_change_1": 0.0,
        "bear_score_change_1": 0.0,
        "confidence_change_1": 0.0,
        "breakout_quality_change_1": 0.0,
        "cvd_change_window": 0.0,
        "decision_stability_score": 0.0,
        "signal_decay_score": 0.0,
        "bias_persistence_count": 0,
        "regime_persistence_count": 0,
        "same_direction_streak": 0,
        "bias_changed": False,
        "regime_changed": False,
        "quality_jump": False,
        "trap_risk_increased": False,
        "no_trade_activated": False,
        "trade_plan_invalidated": False,
    }
