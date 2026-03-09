# BTC PRO V5 snapshot script
# Canvas version for iterative updates.

import json
import os
import ssl
import time
import threading
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

BASE = "https://api.bybit.com"
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

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "btc-pro-v5-snapshot/1.0"})
HISTORY_PATH = os.path.expanduser("~/btc_snapshot_history.jsonl")
MAX_HISTORY_ROWS = 50


def req(url, params=None, timeout=15):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if data.get("retCode") not in (0, "0", None):
        raise RuntimeError(f"API hiba: {data}")
    return data


def f(x, default=None):
    try:
        if x in (None, "", "None"):
            return default
        return float(x)
    except Exception:
        return default


def pct(new, old, default=None):
    if new is None or old in (None, 0):
        return default
    return (new - old) / old * 100.0


def safe_div(a, b, default=None):
    if a is None or b in (None, 0):
        return default
    return a / b


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def fmt_side(side):
    if side in ("Buy", "buy", "B"):
        return "Buy"
    if side in ("Sell", "sell", "S"):
        return "Sell"
    return side


def instrument_info():
    data = req(URL_INSTRUMENTS, {"category": "linear", "symbol": SYMBOL_PERP})
    items = data["result"]["list"]
    if not items:
        return {}
    it = items[0]
    return {
        "funding_interval_min": it.get("fundingInterval"),
        "launch_time_ms": it.get("launchTime"),
        "price_scale": it.get("priceScale"),
    }


def ticker(category, symbol):
    data = req(URL_TICKERS, {"category": category, "symbol": symbol})
    it = data["result"]["list"][0]

    last = f(it.get("lastPrice"))
    bid1 = f(it.get("bid1Price"))
    ask1 = f(it.get("ask1Price"))
    low24 = f(it.get("lowPrice24h"))
    high24 = f(it.get("highPrice24h"))
    prev24 = f(it.get("prevPrice24h"))
    turnover24 = f(it.get("turnover24h"))
    volume24 = f(it.get("volume24h"))

    out = {
        "last": last,
        "bid1": bid1,
        "ask1": ask1,
        "low24": low24,
        "high24": high24,
        "prev24": prev24,
        "chg24_pct": pct(last, prev24),
        "turnover24_usd": turnover24,
        "volume24": volume24,
    }

    if category == "linear":
        mark = f(it.get("markPrice"))
        index = f(it.get("indexPrice"))
        funding = f(it.get("fundingRate"))
        spread_bps = None
        if None not in (bid1, ask1) and (ask1 + bid1) != 0:
            spread_bps = ((ask1 - bid1) / ((ask1 + bid1) / 2.0)) * 10000
        range_pos = None
        if None not in (last, low24, high24) and high24 != low24:
            range_pos = (last - low24) / (high24 - low24) * 100.0
        out.update({
            "mark": mark,
            "index": index,
            "spread_bps": spread_bps,
            "premium_vs_index_pct": pct(mark, index),
            "funding_pct": None if funding is None else funding * 100.0,
            "range_pos_pct": range_pos,
        })
    return out


def funding_history(limit=10):
    data = req(URL_FUNDING, {"category": "linear", "symbol": SYMBOL_PERP, "limit": limit})
    rows = data["result"]["list"]
    vals = []
    for r in rows:
        rate = f(r.get("fundingRate"))
        vals.append(None if rate is None else rate * 100.0)
    latest = vals[0] if len(vals) > 0 else None
    prev = vals[1] if len(vals) > 1 else None
    slope = None if None in (latest, prev) else latest - prev
    accel = None
    if len(vals) > 2 and None not in (vals[0], vals[1], vals[2]):
        accel = (vals[0] - vals[1]) - (vals[1] - vals[2])
    return {
        "funding_history_pct": vals,
        "funding_slope_pct_pt": slope,
        "funding_accel_pct_pt": accel,
    }


def open_interest(interval):
    data = req(URL_OI, {
        "category": "linear",
        "symbol": SYMBOL_PERP,
        "intervalTime": interval,
        "limit": 2,
    })
    rows = data["result"]["list"]
    vals = [f(r.get("openInterest")) for r in rows]
    now = vals[0] if len(vals) > 0 else None
    prev = vals[1] if len(vals) > 1 else None
    return now, pct(now, prev)


def all_oi():
    out = {}
    mapping = {"5min": "5m", "15min": "15m", "30min": "30m", "1h": "1h", "4h": "4h", "1d": "1d"}
    for api_interval, key in mapping.items():
        now, chg = open_interest(api_interval)
        out[f"oi_{key}"] = now
        out[f"oi_change_{key}_pct"] = chg
    return out


def orderbook(category, symbol, limit=200):
    data = req(URL_ORDERBOOK, {"category": category, "symbol": symbol, "limit": limit})
    ob = data["result"]
    bids = [(f(x[0]), f(x[1])) for x in ob["b"]]
    asks = [(f(x[0]), f(x[1])) for x in ob["a"]]
    bids = [(p, q) for p, q in bids if p is not None and q is not None]
    asks = [(p, q) for p, q in asks if p is not None and q is not None]
    if not bids or not asks:
        return {}
    mid = (bids[0][0] + asks[0][0]) / 2.0

    def depth_notional(side_rows, pct_band, side):
        total = 0.0
        if side == "bid":
            threshold = mid * (1 - pct_band / 100.0)
            for p, q in side_rows:
                if p >= threshold:
                    total += p * q
        else:
            threshold = mid * (1 + pct_band / 100.0)
            for p, q in side_rows:
                if p <= threshold:
                    total += p * q
        return total

    def imbalance(pct_band):
        b = depth_notional(bids, pct_band, "bid")
        a = depth_notional(asks, pct_band, "ask")
        if (a + b) == 0:
            return None
        return (b - a) / (b + a) * 100.0

    return {
        "orderbook_mid": mid,
        "orderbook_imbalance_0_10_pct": imbalance(0.10),
        "orderbook_imbalance_0_25_pct": imbalance(0.25),
        "orderbook_imbalance_0_50_pct": imbalance(0.50),
        "orderbook_imbalance_1_00_pct": imbalance(1.00),
    }


def recent_trades(category, symbol, limit=1000):
    data = req(URL_TRADES, {"category": category, "symbol": symbol, "limit": limit})
    rows = data["result"]["list"]

    buy_count = sell_count = 0
    buy_qty = sell_qty = 0.0
    buy_notional = sell_notional = 0.0
    large_100k_buy = large_100k_sell = 0
    large_500k_buy = large_500k_sell = 0
    large_1m_buy = large_1m_sell = 0
    large_100k_buy_usd = large_100k_sell_usd = 0.0
    large_500k_buy_usd = large_500k_sell_usd = 0.0
    large_1m_buy_usd = large_1m_sell_usd = 0.0
    signed_notional = []

    for r in rows:
        side = fmt_side(r.get("side"))
        price = f(r.get("price"), 0.0)
        size = f(r.get("size"), 0.0)
        notion = price * size

        if side == "Buy":
            buy_count += 1
            buy_qty += size
            buy_notional += notion
            signed_notional.append(notion)
            if notion >= 100_000:
                large_100k_buy += 1
                large_100k_buy_usd += notion
            if notion >= 500_000:
                large_500k_buy += 1
                large_500k_buy_usd += notion
            if notion >= 1_000_000:
                large_1m_buy += 1
                large_1m_buy_usd += notion
        elif side == "Sell":
            sell_count += 1
            sell_qty += size
            sell_notional += notion
            signed_notional.append(-notion)
            if notion >= 100_000:
                large_100k_sell += 1
                large_100k_sell_usd += notion
            if notion >= 500_000:
                large_500k_sell += 1
                large_500k_sell_usd += notion
            if notion >= 1_000_000:
                large_1m_sell += 1
                large_1m_sell_usd += notion

    total_count = buy_count + sell_count
    total_qty = buy_qty + sell_qty
    total_notional = buy_notional + sell_notional
    cvd_last = sum(signed_notional) if signed_notional else None
    cvd_last_100 = sum(signed_notional[-100:]) if signed_notional else None
    cvd_last_250 = sum(signed_notional[-250:]) if signed_notional else None
    cvd_trend = None
    if len(signed_notional) >= 2:
        half = len(signed_notional) // 2
        cvd_trend = sum(signed_notional[half:]) - sum(signed_notional[:half])

    return {
        "recent_trades_count": total_count,
        "recent_taker_buy_ratio_pct": None if total_count == 0 else buy_count / total_count * 100.0,
        "recent_buy_notional_usd": buy_notional,
        "recent_sell_notional_usd": sell_notional,
        "recent_notional_delta_pct": None if total_notional == 0 else (buy_notional - sell_notional) / total_notional * 100.0,
        "recent_qty_delta_pct": None if total_qty == 0 else (buy_qty - sell_qty) / total_qty * 100.0,
        "large_100k_buy_count": large_100k_buy,
        "large_100k_sell_count": large_100k_sell,
        "large_100k_buy_usd": large_100k_buy_usd,
        "large_100k_sell_usd": large_100k_sell_usd,
        "large_500k_buy_count": large_500k_buy,
        "large_500k_sell_count": large_500k_sell,
        "large_500k_buy_usd": large_500k_buy_usd,
        "large_500k_sell_usd": large_500k_sell_usd,
        "large_1m_buy_count": large_1m_buy,
        "large_1m_sell_count": large_1m_sell,
        "large_1m_buy_usd": large_1m_buy_usd,
        "large_1m_sell_usd": large_1m_sell_usd,
        "cvd_last_usd": cvd_last,
        "cvd_last_100_usd": cvd_last_100,
        "cvd_last_250_usd": cvd_last_250,
        "cvd_trend_usd": cvd_trend,
    }


def klines(category, symbol, interval, limit):
    data = req(URL_KLINE, {"category": category, "symbol": symbol, "interval": interval, "limit": limit})
    rows = data["result"]["list"]
    candles = []
    for r in rows:
        candles.append({
            "ts": int(r[0]),
            "open": f(r[1]),
            "high": f(r[2]),
            "low": f(r[3]),
            "close": f(r[4]),
            "volume": f(r[5]),
            "turnover": f(r[6]),
        })
    return candles


def avg_turnover_ex_current(candles, lookback):
    vals = [c["turnover"] for c in candles[1:lookback+1] if c["turnover"] is not None]
    return sum(vals) / len(vals) if vals else None


def avg_range_ex_current(candles, lookback):
    vals = [(c["high"] - c["low"]) for c in candles[1:lookback+1] if None not in (c["high"], c["low"])]
    return sum(vals) / len(vals) if vals else None


def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(period):
        cur = candles[i]
        prev = candles[i + 1]
        if None in (cur["high"], cur["low"], prev["close"]):
            continue
        tr = max(cur["high"] - cur["low"], abs(cur["high"] - prev["close"]), abs(cur["low"] - prev["close"]))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else None


def candle_metrics(c):
    if not c:
        return {}
    o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
    if None in (o, h, l, cl):
        return {}
    rng = h - l
    body = abs(cl - o)
    upper_wick = h - max(o, cl)
    lower_wick = min(o, cl) - l
    return {
        "body_pct_of_range": None if rng == 0 else body / rng * 100.0,
        "upper_wick_pct_of_range": None if rng == 0 else upper_wick / rng * 100.0,
        "lower_wick_pct_of_range": None if rng == 0 else lower_wick / rng * 100.0,
        "is_green": cl >= o,
        "range_abs": rng,
    }


def calc_vwap(candles):
    pv = 0.0
    vol = 0.0
    for c in candles:
        if None in (c["high"], c["low"], c["close"], c["volume"]):
            continue
        typical = (c["high"] + c["low"] + c["close"]) / 3.0
        pv += typical * c["volume"]
        vol += c["volume"]
    if vol == 0:
        return None
    return pv / vol


def volume_and_structure(category, symbol):
    k5 = klines(category, symbol, "5", 60)
    k15 = klines(category, symbol, "15", 40)
    k60 = klines(category, symbol, "60", 30)
    cur5 = k5[0] if k5 else None
    cur15 = k15[0] if k15 else None
    cur60 = k60[0] if k60 else None
    t5 = cur5["turnover"] if cur5 else None
    t15 = cur15["turnover"] if cur15 else None
    t60 = cur60["turnover"] if cur60 else None
    avg5 = avg_turnover_ex_current(k5, 12)
    avg15 = avg_turnover_ex_current(k15, 12)
    avg60 = avg_turnover_ex_current(k60, 12)
    avg_range_5m = avg_range_ex_current(k5, 12)
    avg_range_15m = avg_range_ex_current(k15, 12)
    cur5_range = None if not cur5 else (cur5["high"] - cur5["low"])
    cur15_range = None if not cur15 else (cur15["high"] - cur15["low"])
    prev5_high = k5[1]["high"] if len(k5) > 1 else None
    prev5_low = k5[1]["low"] if len(k5) > 1 else None
    prev15_high = k15[1]["high"] if len(k15) > 1 else None
    prev15_low = k15[1]["low"] if len(k15) > 1 else None
    highs_12_5m = [c["high"] for c in k5[:12] if c["high"] is not None]
    lows_12_5m = [c["low"] for c in k5[:12] if c["low"] is not None]
    vwap_1h = calc_vwap(k5[:12])
    vwap_24h = calc_vwap(k60[:24])
    last_price = cur5["close"] if cur5 else None
    atr_5m = calc_atr(k5, 14)
    atr_15m = calc_atr(k15, 14)

    out = {
        "turnover_5m_usd": t5,
        "turnover_15m_usd": t15,
        "turnover_1h_usd": t60,
        "avg_turnover_5m_usd": avg5,
        "avg_turnover_15m_usd": avg15,
        "avg_turnover_1h_usd": avg60,
        "volume_spike_5m_x": safe_div(t5, avg5),
        "volume_spike_15m_x": safe_div(t15, avg15),
        "volume_spike_1h_x": safe_div(t60, avg60),
        "range_5m": cur5_range,
        "range_15m": cur15_range,
        "avg_range_5m": avg_range_5m,
        "avg_range_15m": avg_range_15m,
        "range_expansion_5m_x": safe_div(cur5_range, avg_range_5m),
        "range_expansion_15m_x": safe_div(cur15_range, avg_range_15m),
        "prev_5m_high": prev5_high,
        "prev_5m_low": prev5_low,
        "prev_15m_high": prev15_high,
        "prev_15m_low": prev15_low,
        "swing_high_12x5m": max(highs_12_5m) if highs_12_5m else None,
        "swing_low_12x5m": min(lows_12_5m) if lows_12_5m else None,
        "vwap_1h": vwap_1h,
        "vwap_24h": vwap_24h,
        "price_vs_vwap_1h_pct": pct(last_price, vwap_1h),
        "price_vs_vwap_24h_pct": pct(last_price, vwap_24h),
        "atr_5m": atr_5m,
        "atr_15m": atr_15m,
    }

    out.update({f"cur5m_{k}": v for k, v in candle_metrics(cur5).items()})
    out.update({f"cur15m_{k}": v for k, v in candle_metrics(cur15).items()})
    return out


def spot_perp_divergence():
    perp = ticker("linear", SYMBOL_PERP)
    spot = ticker("spot", SYMBOL_SPOT)
    perp_k5 = klines("linear", SYMBOL_PERP, "5", 2)
    spot_k5 = klines("spot", SYMBOL_SPOT, "5", 2)
    perp_chg_5m = None
    spot_chg_5m = None
    if len(perp_k5) > 1:
        perp_chg_5m = pct(perp_k5[0]["close"], perp_k5[1]["close"])
    if len(spot_k5) > 1:
        spot_chg_5m = pct(spot_k5[0]["close"], spot_k5[1]["close"])
    return {
        "spot_last": spot.get("last"),
        "spot_chg24_pct": spot.get("chg24_pct"),
        "spot_turnover24_usd": spot.get("turnover24_usd"),
        "spot_volume24_btc": spot.get("volume24"),
        "perp_spot_last_spread_pct": pct(perp.get("last"), spot.get("last")),
        "perp_5m_chg_pct": perp_chg_5m,
        "spot_5m_chg_pct": spot_chg_5m,
        "spot_perp_divergence_5m_pct_pt": None if None in (perp_chg_5m, spot_chg_5m) else (perp_chg_5m - spot_chg_5m),
    }


def liquidation_tracker_best_effort(duration_sec=8):
    out = {
        "liq_capture_window_sec": duration_sec,
        "liq_available": False,
        "long_liq_usd_5m": None,
        "short_liq_usd_5m": None,
        "liq_imbalance_pct": None,
    }
    try:
        import websocket
    except Exception:
        return out

    collected = []

    def on_message(ws, message):
        try:
            data = json.loads(message)
            topic = data.get("topic", "")
            if "liquidation" not in topic.lower():
                return
            rows = data.get("data", [])
            if isinstance(rows, dict):
                rows = [rows]
            for r in rows:
                if r.get("symbol") != SYMBOL_PERP:
                    continue
                side = fmt_side(r.get("side"))
                price = f(r.get("price"), 0.0)
                size = f(r.get("size"), 0.0)
                collected.append({"side": side, "usd": price * size})
        except Exception:
            pass

    def on_open(ws):
        try:
            ws.send(json.dumps({
                "op": "subscribe",
                "args": ["liquidation.BTCUSDT", "allLiquidation.BTCUSDT"]
            }))
        except Exception:
            pass

    def runner():
        ws = websocket.WebSocketApp(WS_PUBLIC, on_open=on_open, on_message=on_message)
        try:
            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=20, ping_timeout=10)
        except Exception:
            pass

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    time.sleep(duration_sec)

    long_liq = 0.0
    short_liq = 0.0
    for x in collected:
        if x["side"] == "Sell":
            long_liq += x["usd"]
        elif x["side"] == "Buy":
            short_liq += x["usd"]

    total = long_liq + short_liq
    imbalance = None if total == 0 else (short_liq - long_liq) / total * 100.0
    out.update({
        "liq_available": True,
        "long_liq_usd_5m": long_liq,
        "short_liq_usd_5m": short_liq,
        "liq_imbalance_pct": imbalance,
    })
    return out


def load_history(path=HISTORY_PATH, max_rows=MAX_HISTORY_ROWS):
    rows = []
    if not os.path.exists(path):
        return rows
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return []
    return rows[-max_rows:]


def append_history(snapshot, path=HISTORY_PATH, max_rows=MAX_HISTORY_ROWS):
    rows = load_history(path, max_rows)
    rows.append(snapshot)
    rows = rows[-max_rows:]
    try:
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass


def compare_to_previous(snapshot, history_rows):
    btc = snapshot.get("btc", {})
    prev = history_rows[-1] if history_rows else None
    prev_btc = prev.get("btc", {}) if prev else {}

    def prevv(key):
        return prev_btc.get(key)

    out = {
        "history_rows_loaded": len(history_rows),
        "prev_trade_bias": prevv("trade_bias"),
        "prev_market_regime": prevv("market_regime"),
        "prev_breakout_quality_score": prevv("breakout_quality_score"),
        "prev_bull_score": prevv("bull_score"),
        "prev_bear_score": prevv("bear_score"),
        "bull_score_change_1": None,
        "bear_score_change_1": None,
        "confidence_change_1": None,
        "breakout_quality_change_1": None,
        "cvd_change_window": None,
        "decision_stability_score": None,
        "signal_decay_score": None,
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

    if prev:
        if btc.get("bull_score") is not None and prevv("bull_score") is not None:
            out["bull_score_change_1"] = btc["bull_score"] - prevv("bull_score")
        if btc.get("bear_score") is not None and prevv("bear_score") is not None:
            out["bear_score_change_1"] = btc["bear_score"] - prevv("bear_score")
        if btc.get("confidence_score") is not None and prevv("confidence_score") is not None:
            out["confidence_change_1"] = btc["confidence_score"] - prevv("confidence_score")
        if btc.get("breakout_quality_score") is not None and prevv("breakout_quality_score") is not None:
            out["breakout_quality_change_1"] = btc["breakout_quality_score"] - prevv("breakout_quality_score")
        if btc.get("cvd_trend_usd") is not None and prevv("cvd_trend_usd") is not None:
            out["cvd_change_window"] = btc["cvd_trend_usd"] - prevv("cvd_trend_usd")

        out["bias_changed"] = btc.get("trade_bias") != prevv("trade_bias")
        out["regime_changed"] = btc.get("market_regime") != prevv("market_regime")
        if out["breakout_quality_change_1"] is not None and out["breakout_quality_change_1"] >= 15:
            out["quality_jump"] = True
        prev_trap = max(prevv("bull_trap_risk") or 0, prevv("bear_trap_risk") or 0)
        cur_trap = max(btc.get("bull_trap_risk") or 0, btc.get("bear_trap_risk") or 0)
        out["trap_risk_increased"] = cur_trap - prev_trap >= 20
        out["no_trade_activated"] = (prevv("no_trade_active") is False) and (btc.get("no_trade_active") is True)
        prev_plan = prevv("trade_plan_side")
        cur_plan = btc.get("trade_plan_side")
        out["trade_plan_invalidated"] = prev_plan in ("long", "short") and cur_plan == "no_trade"

    # persistence counts from newest backwards
    cur_bias = btc.get("trade_bias")
    cur_regime = btc.get("market_regime")
    cur_dir = btc.get("breakout_direction")
    for row in reversed(history_rows):
        rb = row.get("btc", {}).get("trade_bias")
        if rb == cur_bias:
            out["bias_persistence_count"] += 1
        else:
            break
    for row in reversed(history_rows):
        rr = row.get("btc", {}).get("market_regime")
        if rr == cur_regime:
            out["regime_persistence_count"] += 1
        else:
            break
    for row in reversed(history_rows):
        rd = row.get("btc", {}).get("breakout_direction")
        if rd == cur_dir and rd is not None:
            out["same_direction_streak"] += 1
        else:
            break

    # stability: how often recent bias changes
    recent_biases = [r.get("btc", {}).get("trade_bias") for r in history_rows[-10:]] + [cur_bias]
    recent_biases = [x for x in recent_biases if x is not None]
    switches = 0
    for i in range(1, len(recent_biases)):
        if recent_biases[i] != recent_biases[i-1]:
            switches += 1
    stability = max(0, 100 - switches * 15)
    out["decision_stability_score"] = stability

    # signal decay: bull and bear converging or quality falling
    decay = 0
    if out["confidence_change_1"] is not None and out["confidence_change_1"] < 0:
        decay += min(abs(out["confidence_change_1"]) * 2, 40)
    if out["breakout_quality_change_1"] is not None and out["breakout_quality_change_1"] < 0:
        decay += min(abs(out["breakout_quality_change_1"]), 40)
    out["signal_decay_score"] = round(min(decay, 100), 2)

    return out


def global_data():
    r = SESSION.get(URL_GLOBAL, timeout=12)
    r.raise_for_status()
    d = r.json()["data"]
    return {
        "total_mcap_usd": f(d["total_market_cap"]["usd"]),
        "btc_dom_pct": f(d["market_cap_percentage"]["btc"]),
    }


def session_context(now_dt):
    hour = now_dt.hour
    if now_dt.weekday() >= 5:
        session_name = "weekend"
        liquidity_profile = "thin"
    elif 0 <= hour < 7:
        session_name = "asia"
        liquidity_profile = "medium"
    elif 7 <= hour < 13:
        session_name = "europe"
        liquidity_profile = "medium_high"
    elif 13 <= hour < 21:
        session_name = "us"
        liquidity_profile = "high"
    else:
        session_name = "after_hours"
        liquidity_profile = "medium_low"
    return {
        "session_name": session_name,
        "session_liquidity_profile": liquidity_profile,
    }


def absorption_detector(d):
    buy_absorption = False
    sell_absorption = False
    reason = []

    delta = d.get("recent_notional_delta_pct")
    range5 = d.get("range_expansion_5m_x")
    body5 = d.get("cur5m_body_pct_of_range")
    lower_wick = d.get("cur5m_lower_wick_pct_of_range")
    upper_wick = d.get("cur5m_upper_wick_pct_of_range")
    price_vwap = d.get("price_vs_vwap_1h_pct")

    if None not in (delta, range5, lower_wick, body5):
        if delta < -10 and range5 < 1.0 and lower_wick > 35 and body5 < 60:
            buy_absorption = True
            reason.append("sell_flow_absorbed")

    if None not in (delta, range5, upper_wick, body5):
        if delta > 10 and range5 < 1.0 and upper_wick > 35 and body5 < 60:
            sell_absorption = True
            reason.append("buy_flow_absorbed")

    if price_vwap is not None:
        if buy_absorption and price_vwap < 0:
            reason.append("below_vwap_supportive")
        if sell_absorption and price_vwap > 0:
            reason.append("above_vwap_resistive")

    return {
        "absorption_buying": buy_absorption,
        "absorption_selling": sell_absorption,
        "absorption_reason": reason,
    }


def delta_divergence_detector(d):
    bull_div = False
    bear_div = False
    notes = []

    delta = d.get("recent_notional_delta_pct")
    cvd = d.get("cvd_trend_usd")
    price_vwap = d.get("price_vs_vwap_1h_pct")
    range_pos = d.get("range_pos_pct")
    oi5 = d.get("oi_change_5m_pct")

    if None not in (delta, cvd, price_vwap, range_pos):
        if range_pos < 35 and delta < 0 and cvd > 0:
            bull_div = True
            notes.append("cvd_holds_while_price_low")
        if range_pos > 65 and delta > 0 and cvd < 0:
            bear_div = True
            notes.append("cvd_fades_while_price_high")

    if bull_div and oi5 is not None and oi5 < 0:
        notes.append("bull_div_with_oi_flush")
    if bear_div and oi5 is not None and oi5 > 0:
        notes.append("bear_div_with_oi_build")

    return {
        "bull_divergence": bull_div,
        "bear_divergence": bear_div,
        "divergence_notes": notes,
    }


def data_quality(snapshot):
    missing = []
    btc = snapshot.get("btc", {})

    required_fields = [
        "last", "funding_pct", "oi_change_5m_pct", "recent_notional_delta_pct",
        "volume_spike_5m_x", "orderbook_imbalance_0_25_pct", "vwap_1h"
    ]
    for k in required_fields:
        if btc.get(k) is None:
            missing.append(k)

    score = 100
    score -= len(missing) * 8

    if btc.get("liq_available") is False:
        score -= 10
    if btc.get("spread_bps") is not None and btc["spread_bps"] > 5:
        score -= 5

    score = max(score, 0)
    return {
        "data_quality_score": score,
        "missing_modules": missing,
        "ws_liq_status": "ok" if btc.get("liq_available") else "missing_or_inactive",
    }


def multi_tf_trend(d):
    trend_5m = "range"
    trend_15m = "range"
    trend_1h = "range"

    p_vwap_1h = d.get("price_vs_vwap_1h_pct")
    p_vwap_24h = d.get("price_vs_vwap_24h_pct")
    delta = d.get("recent_notional_delta_pct")
    oi5 = d.get("oi_change_5m_pct")
    oi15 = d.get("oi_change_15m_pct")
    vol5 = d.get("volume_spike_5m_x")
    vol15 = d.get("volume_spike_15m_x")

    if p_vwap_1h is not None and delta is not None:
        if p_vwap_1h > 0 and delta > 5:
            trend_5m = "up"
        elif p_vwap_1h < 0 and delta < -5:
            trend_5m = "down"

    if p_vwap_1h is not None and oi15 is not None and vol15 is not None:
        if p_vwap_1h > 0 and vol15 > 1.0 and oi15 <= 0.25:
            trend_15m = "up"
        elif p_vwap_1h < 0 and vol15 > 1.0 and oi15 >= -0.25:
            trend_15m = "down"

    if p_vwap_24h is not None:
        if p_vwap_24h > 0.15:
            trend_1h = "up"
        elif p_vwap_24h < -0.15:
            trend_1h = "down"

    alignment_score = 0
    for t in (trend_5m, trend_15m, trend_1h):
        if t == "up":
            alignment_score += 1
        elif t == "down":
            alignment_score -= 1

    return {
        "trend_5m": trend_5m,
        "trend_15m": trend_15m,
        "trend_1h": trend_1h,
        "multi_tf_alignment_score": alignment_score,
    }


def liquidity_map_proxy(d):
    above = []
    below = []
    last = d.get("last")
    for key in ["prev_5m_high", "prev_15m_high", "swing_high_12x5m", "vwap_24h"]:
        v = d.get(key)
        if v is not None and last is not None and v > last:
            above.append((abs(v - last), v, key))
    for key in ["prev_5m_low", "prev_15m_low", "swing_low_12x5m", "vwap_24h", "vwap_1h"]:
        v = d.get(key)
        if v is not None and last is not None and v < last:
            below.append((abs(v - last), v, key))
    above.sort(key=lambda x: x[0])
    below.sort(key=lambda x: x[0])

    liq_above_1 = above[0][1] if len(above) > 0 else None
    liq_above_2 = above[1][1] if len(above) > 1 else None
    liq_below_1 = below[0][1] if len(below) > 0 else None
    liq_below_2 = below[1][1] if len(below) > 1 else None

    nearest_side = "neutral"
    if last is not None and liq_above_1 is not None and liq_below_1 is not None:
        if abs(liq_above_1 - last) < abs(last - liq_below_1):
            nearest_side = "above"
        elif abs(liq_above_1 - last) > abs(last - liq_below_1):
            nearest_side = "below"

    return {
        "liq_above_1": liq_above_1,
        "liq_above_2": liq_above_2,
        "liq_below_1": liq_below_1,
        "liq_below_2": liq_below_2,
        "nearest_liquidity_side": nearest_side,
    }


def orderbook_wall_tracker(d):
    mid = d.get("orderbook_mid")
    bid_wall_price = None
    ask_wall_price = None
    bid_wall_usd = None
    ask_wall_usd = None
    wall_pressure = "neutral"

    # proxy from nearest liquidity and book imbalance, since full level-by-level wall ids are not retained
    if d.get("liq_below_1") is not None:
        bid_wall_price = d.get("liq_below_1")
        if mid is not None:
            bid_wall_usd = abs(mid - bid_wall_price) * 1000
    if d.get("liq_above_1") is not None:
        ask_wall_price = d.get("liq_above_1")
        if mid is not None:
            ask_wall_usd = abs(ask_wall_price - mid) * 1000

    ob = d.get("orderbook_imbalance_0_25_pct")
    if ob is not None:
        if ob > 8:
            wall_pressure = "bid"
        elif ob < -8:
            wall_pressure = "ask"

    return {
        "largest_bid_wall_price": bid_wall_price,
        "largest_ask_wall_price": ask_wall_price,
        "largest_bid_wall_usd": bid_wall_usd,
        "largest_ask_wall_usd": ask_wall_usd,
        "wall_pressure_side": wall_pressure,
    }


def compute_scores(d):
    bull = 0.0
    bear = 0.0
    funding = d.get("funding_pct")
    regime = d.get("market_regime")
    session_name = d.get("session_name")

    if funding is not None:
        if funding < 0:
            bull += min(abs(funding) * 1200, 18)
        elif funding > 0:
            bear += min(abs(funding) * 1200, 18)

    delta = d.get("recent_notional_delta_pct")
    if delta is not None:
        delta_mult = 0.9
        if regime in ("trend_build_long", "trend_build_short"):
            delta_mult = 1.1
        elif regime in ("low_liquidity_range", "neutral") and session_name == "weekend":
            delta_mult = 0.7
        if delta > 0:
            bull += min(delta * delta_mult, 20)
        elif delta < 0:
            bear += min(abs(delta) * delta_mult, 20)

    cvd = d.get("cvd_trend_usd")
    if cvd is not None:
        cvd_div = 50000.0
        if regime in ("trend_build_long", "trend_build_short"):
            cvd_div = 40000.0
        elif regime == "low_liquidity_range":
            cvd_div = 70000.0
        if cvd > 0:
            bull += min(cvd / cvd_div, 12)
        elif cvd < 0:
            bear += min(abs(cvd) / cvd_div, 12)

    oi5 = d.get("oi_change_5m_pct")
    vol5 = d.get("volume_spike_5m_x")
    if oi5 is not None and oi5 < -0.5:
        down += 10
    if oi5 is not None and oi5 > 0.5:
        up += 10

    up = round(clamp(up, 0, 100), 2)
    down = round(clamp(down, 0, 100), 2)

    return {
        "move_exhaustion_up": up,
        "move_exhaustion_down": down,
    }
