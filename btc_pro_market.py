# btc_pro_market.py
import statistics
from datetime import datetime
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


def _f(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _i(x, default=0):
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def _pct_change(new, old, default=0.0):
    new = _f(new, None)
    old = _f(old, None)
    if new is None or old in (None, 0):
        return default
    return ((new / old) - 1.0) * 100.0


def _safe_div(a, b, default=0.0):
    a = _f(a, None)
    b = _f(b, None)
    if a is None or b in (None, 0):
        return default
    return a / b


def _median(vals, default=0.0):
    vals = [float(v) for v in vals if v is not None]
    if not vals:
        return default
    return statistics.median(vals)


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


def _now_bucharest():
    return datetime.now(ZoneInfo("Europe/Bucharest")).strftime("%Y-%m-%d %H:%M:%S")


def instrument_info(symbol=SYMBOL):
    items = req_bybit(URL_INSTRUMENTS, {"category": "linear", "symbol": symbol})["result"]["list"]
    item = items[0] if items else {}
    return {
        "funding_interval_min": _i(item.get("fundingInterval"), 480),
        "launch_time_ms": str(item.get("launchTime", "")),
        "price_scale": str(item.get("priceScale", "")),
        "source_instrument_info": "bybit",
    }


def ticker(symbol=SYMBOL):
    items = req_bybit(URL_TICKERS, {"category": "linear", "symbol": symbol})["result"]["list"]
    item = items[0] if items else {}

    last = _f(item.get("lastPrice"))
    bid1 = _f(item.get("bid1Price"))
    ask1 = _f(item.get("ask1Price"))
    low24 = _f(item.get("lowPrice24h"))
    high24 = _f(item.get("highPrice24h"))
    prev24 = _f(item.get("prevPrice24h"))
    mark = _f(item.get("markPrice"))
    index = _f(item.get("indexPrice"))

    return {
        "last": last,
        "bid1": bid1,
        "ask1": ask1,
        "low24": low24,
        "high24": high24,
        "prev24": prev24,
        "chg24_pct": _f(item.get("price24hPcnt")) * 100.0,
        "turnover24_usd": _f(item.get("turnover24h")),
        "volume24": _f(item.get("volume24h")),
        "mark": mark,
        "index": index,
        "funding_pct": _f(item.get("fundingRate")) * 100.0,
        "open_interest": _f(item.get("openInterest")),
        "spread_bps": 0.0 if last == 0 else ((ask1 - bid1) / last) * 10000.0,
        "premium_vs_index_pct": _pct_change(mark, index, 0.0),
        "range_pos_pct": 0.0 if high24 == low24 else ((last - low24) / (high24 - low24)) * 100.0,
        "source_ticker": "bybit",
    }


def funding_history(symbol=SYMBOL, limit=10):
    data = req_bybit(
        URL_FUNDING_HISTORY,
        {"category": "linear", "symbol": symbol, "limit": limit},
    )["result"]["list"]

    vals = [_f(x.get("fundingRate")) * 100.0 for x in data]
    vals = list(reversed(vals))

    slope = 0.0
    accel = 0.0
    if len(vals) >= 2:
        slope = vals[-1] - vals[-2]
    if len(vals) >= 3:
        accel = (vals[-1] - vals[-2]) - (vals[-2] - vals[-3])

    cur = vals[-1] if vals else 0.0

    return {
        "funding_pct": cur,
        "funding_history_pct": vals,
        "funding_slope_pct_pt": slope,
        "funding_accel_pct_pt": accel,
        "source_funding": "bybit",
    }


def all_oi(symbol=SYMBOL):
    t = ticker(symbol)
    oi_now = _f(t.get("open_interest"))

    return {
        "oi_5m": oi_now,
        "oi_change_5m_pct": 0.0,
        "oi_15m": oi_now,
        "oi_change_15m_pct": 0.0,
        "oi_30m": oi_now,
        "oi_change_30m_pct": 0.0,
        "oi_1h": oi_now,
        "oi_change_1h_pct": 0.0,
        "oi_4h": oi_now,
        "oi_change_4h_pct": 0.0,
        "oi_1d": oi_now,
        "oi_change_1d_pct": 0.0,
        "source_open_interest": "bybit",
    }


def orderbook(symbol=SYMBOL, limit=50):
    data = req_bybit(
        URL_ORDERBOOK,
        {"category": "linear", "symbol": symbol, "limit": limit},
    )["result"]

    bids = [(_f(p), _f(q)) for p, q in (data.get("b", []) or [])]
    asks = [(_f(p), _f(q)) for p, q in (data.get("a", []) or [])]

    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 0.0
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

    out = {
        "orderbook_mid": mid,
        "source_orderbook": "bybit",
    }

    for pct, name in [
        (0.001, "0_10"),
        (0.0025, "0_25"),
        (0.005, "0_50"),
        (0.01, "1_00"),
    ]:
        bid_notional = side_notional(bids, pct)
        ask_notional = side_notional(asks, pct)
        total = bid_notional + ask_notional
        imbalance = 0.0 if total == 0 else ((bid_notional - ask_notional) / total) * 100.0
        out[f"orderbook_imbalance_{name}_pct"] = imbalance

    if bids:
        lb = max(bids, key=lambda x: x[0] * x[1])
        out["largest_bid_wall_price"] = lb[0]
        out["largest_bid_wall_usd"] = lb[0] * lb[1]
    else:
        out["largest_bid_wall_price"] = 0.0
        out["largest_bid_wall_usd"] = 0.0

    if asks:
        la = max(asks, key=lambda x: x[0] * x[1])
        out["largest_ask_wall_price"] = la[0]
        out["largest_ask_wall_usd"] = la[0] * la[1]
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


def recent_trades(symbol=SYMBOL, limit=1000):
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
        price = _f(row.get("price"))
        qty = _f(row.get("size"))
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


def _bybit_klines(symbol=SYMBOL, interval="5", limit=60):
    data = req_bybit(
        URL_KLINE,
        {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit},
    )["result"]["list"]

    rows = []
    for row in reversed(data):
        ts, o, h, l, c, v, t = row[:7]
        rows.append({
            "ts": _i(ts),
            "open": _f(o),
            "high": _f(h),
            "low": _f(l),
            "close": _f(c),
            "volume": _f(v),
            "turnover": _f(t),
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
    return _median(trs[-period:], 0.0)


def _vwap(rows):
    num = 0.0
    den = 0.0
    for r in rows:
        typical = (r["high"] + r["low"] + r["close"]) / 3.0
        num += typical * r["volume"]
        den += r["volume"]
    return 0.0 if den == 0 else num / den


def _trend_label(closes):
    if len(closes) < 4:
        return "range"
    last = closes[-1]
    sma3 = sum(closes[-3:]) / 3.0
    sma6 = sum(closes[-6:]) / 6.0 if len(closes) >= 6 else sum(closes) / len(closes)
    if last > sma3 > sma6:
        return "up"
    if last < sma3 < sma6:
        return "down"
    return "range"


def _candle_stats(row):
    o = _f(row.get("open"))
    h = _f(row.get("high"))
    l = _f(row.get("low"))
    c = _f(row.get("close"))
    r = h - l
    if r <= 0:
        return 0.0, 0.0, 0.0, False, 0.0
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    return (body / r) * 100.0, (upper / r) * 100.0, (lower / r) * 100.0, c >= o, r


def volume_and_structure(symbol=SYMBOL):
    k5 = _bybit_klines(symbol, "5", 60)
    k15 = _bybit_klines(symbol, "15", 60)
    k60 = _bybit_klines(symbol, "60", 60)

    last5 = k5[-1] if k5 else {}
    last15 = k15[-1] if k15 else {}
    prev5 = k5[-2] if len(k5) >= 2 else {}
    prev15 = k15[-2] if len(k15) >= 2 else {}

    closes5 = [x["close"] for x in k5]
    closes15 = [x["close"] for x in k15]
    closes60 = [x["close"] for x in k60]

    med_turnover_5 = _median([x["turnover"] for x in k5[-20:-1]], 0.0)
    med_turnover_15 = _median([x["turnover"] for x in k15[-20:-1]], 0.0)
    med_turnover_1h = _median([x["turnover"] for x in k60[-20:-1]], 0.0)

    med_range_5 = _median([(x["high"] - x["low"]) for x in k5[-20:-1]], 0.0)
    med_range_15 = _median([(x["high"] - x["low"]) for x in k15[-20:-1]], 0.0)

    range_5 = _f(last5.get("high")) - _f(last5.get("low"))
    range_15 = _f(last15.get("high")) - _f(last15.get("low"))

    c5_body, c5_up, c5_low, c5_green, c5_range = _candle_stats(last5)
    c15_body, c15_up, c15_low, c15_green, c15_range = _candle_stats(last15)

    highs12 = [x["high"] for x in k5[-12:]] if len(k5) >= 12 else [x["high"] for x in k5]
    lows12 = [x["low"] for x in k5[-12:]] if len(k5) >= 12 else [x["low"] for x in k5]

    turns_5 = [x["turnover"] for x in k5[-12:]]
    turns_15 = [x["turnover"] for x in k15[-12:]]

    return {
        "turnover_5m_usd": _f(last5.get("turnover")),
        "turnover_15m_usd": _f(last15.get("turnover")),
        "turnover_1h_usd": _f(k60[-1]["turnover"]) if k60 else 0.0,
        "median_turnover_5m_usd": med_turnover_5,
        "median_turnover_15m_usd": med_turnover_15,
        "median_turnover_1h_usd": med_turnover_1h,
        "volume_spike_5m_x": _safe_div(_f(last5.get("turnover")), med_turnover_5, 0.0),
        "volume_spike_15m_x": _safe_div(_f(last15.get("turnover")), med_turnover_15, 0.0),
        "volume_spike_1h_x": _safe_div(_f(k60[-1]["turnover"]) if k60 else 0.0, med_turnover_1h, 0.0),
        "range_5m": range_5,
        "range_15m": range_15,
        "median_range_5m": med_range_5,
        "median_range_15m": med_range_15,
        "range_expansion_5m_x": _safe_div(range_5, med_range_5, 0.0),
        "range_expansion_15m_x": _safe_div(range_15, med_range_15, 0.0),
        "prev_5m_high": _f(prev5.get("high")),
        "prev_5m_low": _f(prev5.get("low")),
        "prev_15m_high": _f(prev15.get("high")),
        "prev_15m_low": _f(prev15.get("low")),
        "swing_high_12x5m": max(highs12) if highs12 else 0.0,
        "swing_low_12x5m": min(lows12) if lows12 else 0.0,
        "vwap_1h": _vwap(k5[-12:]) if len(k5) >= 12 else _vwap(k5),
        "vwap_24h": _vwap(k60[-24:]) if len(k60) >= 24 else _vwap(k60),
        "atr_5m": _atr(k5, 14),
        "atr_15m": _atr(k15, 14),
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
        "trend_5m": _trend_label(closes5),
        "trend_15m": _trend_label(closes15),
        "trend_1h": _trend_label(closes60),
    }


def spot_perp_divergence(symbol=SYMBOL):
    t24 = req_binance(URL_BINANCE_TICKER_24H, {"symbol": symbol})
    spot_last_row = req_binance(URL_BINANCE_TICKER_PRICE, {"symbol": symbol})
    kl5 = req_binance(URL_BINANCE_KLINES, {"symbol": symbol, "interval": "5m", "limit": 3})

    spot_last = _f(spot_last_row.get("price"))
    spot_chg24_pct = _f(t24.get("priceChangePercent"))
    spot_turnover24 = _f(t24.get("quoteVolume"))
    spot_volume24 = _f(t24.get("volume"))

    close_prev = _f(kl5[-2][4]) if len(kl5) >= 2 else spot_last
    close_last = _f(kl5[-1][4]) if len(kl5) >= 1 else spot_last
    spot_5m_chg = _pct_change(close_last, close_prev, 0.0)

    perp = ticker(symbol)
    perp_last = _f(perp.get("last"))
    perp_5m_chg = _pct_change(perp_last, _f(perp_last) - 1.0, 0.0)

    return {
        "spot_last": spot_last,
        "spot_chg24_pct": spot_chg24_pct,
        "spot_turnover24_usd": spot_turnover24,
        "spot_volume24_btc": spot_volume24,
        "perp_spot_last_spread_pct": _pct_change(perp_last, spot_last, 0.0),
        "perp_5m_chg_pct": perp_5m_chg,
        "spot_5m_chg_pct": spot_5m_chg,
        "spot_perp_divergence_5m_pct_pt": perp_5m_chg - spot_5m_chg,
        "source_perp_context": "bybit",
        "source_spot_context": "binance",
        "source_spot_klines": "binance",
    }


def liquidation_tracker_best_effort(symbol=SYMBOL):
    return {
        "liq_capture_window_sec": 8,
        "liq_available": False,
        "long_liq_usd_5m": None,
        "short_liq_usd_5m": None,
        "liq_imbalance_pct": None,
    }


def global_data():
    data = req(URL_GLOBAL)
    d = data.get("data", {})
    return {
        "total_mcap_usd": _f(d.get("total_market_cap", {}).get("usd")),
        "btc_dom_pct": _f(d.get("market_cap_percentage", {}).get("btc")),
    }
