import json, re, ssl, threading, time
from statistics import median
from btc_pro_config import (
    BINANCE_URL_BOOK_TICKER,
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


def req(url, params=None, timeout=15):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get('retCode') not in (0, '0', None):
        raise RuntimeError(f'API hiba: {data}')
    return data


def req_bybit(url, params=None, timeout=15):
    return req(url, params=params, timeout=timeout)


def req_binance(url, params=None, timeout=15):
    return req(url, params=params, timeout=timeout)

def req_deribit(url, params=None, timeout=15):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get('error'):
        raise RuntimeError(f"Deribit API hiba: {data['error']}")
    return data.get('result', data)


def _as_rows(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if isinstance(payload.get('result'), list):
            return payload['result']
        return [payload]
    return []


def _extract_number_near_label(html, labels):
    if not html:
        return None
    candidates = []
    for label in labels:
        patterns = [
            rf"{label}[^0-9]{{0,80}}([0-9][0-9,\.]+)",
            rf"([0-9][0-9,\.]+)[^<]{{0,80}}{label}",
        ]
        for pat in patterns:
            m = re.search(pat, html, flags=re.I)
            if m:
                raw = m.group(1).replace(',', '')
                try:
                    candidates.append(float(raw))
                except Exception:
                    pass
    return candidates[0] if candidates else None


def deribit_btc_context():
    out = {
        'deribit_perp_last': None,
        'deribit_perp_bid': None,
        'deribit_perp_ask': None,
        'deribit_perp_mark': None,
        'deribit_perp_index': None,
        'deribit_basis_bps': None,
        'deribit_futures_oi_total': None,
        'deribit_futures_volume_24h': None,
        'deribit_option_put_oi': None,
        'deribit_option_call_oi': None,
        'deribit_put_call_oi_ratio': None,
        'deribit_market_bias': 'neutral',
        'deribit_confirmation_score': 50.0,
        'source_deribit_context': 'unavailable',
    }
    try:
        perp_rows = _as_rows(req_deribit(DERIBIT_URL_BOOK_SUMMARY_BY_INSTRUMENT, {'instrument_name': DERIBIT_BTC_PERP}))
        fut_rows = _as_rows(req_deribit(DERIBIT_URL_BOOK_SUMMARY_BY_CURRENCY, {'currency': 'BTC', 'kind': 'future'}))
        opt_rows = _as_rows(req_deribit(DERIBIT_URL_BOOK_SUMMARY_BY_CURRENCY, {'currency': 'BTC', 'kind': 'option'}))
    except Exception:
        return out

    perp = perp_rows[0] if perp_rows else {}
    out['deribit_perp_last'] = f(perp.get('last'))
    out['deribit_perp_bid'] = f(perp.get('bid_price'))
    out['deribit_perp_ask'] = f(perp.get('ask_price'))
    out['deribit_perp_mark'] = f(perp.get('mark_price'))
    out['deribit_perp_index'] = f(perp.get('underlying_index') or perp.get('index_price'))
    if out['deribit_perp_mark'] is not None and out['deribit_perp_index'] not in (None, 0):
        out['deribit_basis_bps'] = (out['deribit_perp_mark'] - out['deribit_perp_index']) / out['deribit_perp_index'] * 10000.0

    fut_oi = []
    fut_vol = []
    for row in fut_rows:
        oi = f(row.get('open_interest'))
        vol = f(row.get('volume') or row.get('volume_usd'))
        if oi is not None:
            fut_oi.append(oi)
        if vol is not None:
            fut_vol.append(vol)
    out['deribit_futures_oi_total'] = sum(fut_oi) if fut_oi else None
    out['deribit_futures_volume_24h'] = sum(fut_vol) if fut_vol else None

    put_oi = call_oi = 0.0
    saw_option = False
    for row in opt_rows:
        oi = f(row.get('open_interest'), 0.0)
        name = (row.get('instrument_name') or '').upper()
        option_type = (row.get('option_type') or '').lower()
        if option_type == 'put' or name.endswith('-P'):
            put_oi += oi; saw_option = True
        elif option_type == 'call' or name.endswith('-C'):
            call_oi += oi; saw_option = True
    out['deribit_option_put_oi'] = put_oi if saw_option else None
    out['deribit_option_call_oi'] = call_oi if saw_option else None
    if saw_option and call_oi > 0:
        out['deribit_put_call_oi_ratio'] = put_oi / call_oi

    score = 50.0
    basis = out['deribit_basis_bps']
    pc = out['deribit_put_call_oi_ratio']
    oi_total = out['deribit_futures_oi_total']
    if basis is not None:
        score += max(-12.0, min(12.0, basis * 0.18))
    if pc is not None:
        if pc < 0.9:
            score += min((0.9 - pc) * 25.0, 10.0)
        elif pc > 1.1:
            score -= min((pc - 1.1) * 25.0, 10.0)
    if oi_total is not None and oi_total > 0:
        score += min(max((oi_total ** 0.5) / 500.0, 0.0), 8.0)
    score = round(max(0.0, min(100.0, score)), 2)
    out['deribit_confirmation_score'] = score
    out['deribit_market_bias'] = 'long' if score >= 56 else 'short' if score <= 44 else 'neutral'
    out['source_deribit_context'] = 'deribit'
    return out


def cme_btc_context():
    out = {
        'cme_btc_volume': None,
        'cme_btc_open_interest': None,
        'cme_reference_context_available': False,
        'cme_market_bias': 'neutral',
        'cme_confirmation_score': 50.0,
        'source_cme_context': 'unavailable',
    }
    html_parts = []
    try:
        html_parts.append(SESSION.get(CME_BTC_VOLUME_OI_URL, timeout=15).text)
    except Exception:
        pass
    try:
        html_parts.append(SESSION.get(CME_BTC_BENCHMARK_URL, timeout=15).text)
    except Exception:
        pass
    html = '\n'.join(part for part in html_parts if part)
    if not html:
        return out
    vol = _extract_number_near_label(html, ['Volume', 'VOL'])
    oi = _extract_number_near_label(html, ['Open Interest', 'OI'])
    out['cme_btc_volume'] = vol
    out['cme_btc_open_interest'] = oi
    out['cme_reference_context_available'] = True
    score = 50.0
    if oi is not None:
        score += min(max((oi ** 0.5) / 200.0, 0.0), 8.0)
    if vol is not None:
        score += min(max((vol ** 0.5) / 200.0, 0.0), 6.0)
    html_lower = html.lower()
    if 'transparency' in html_lower or 'regulated' in html_lower:
        score += 4.0
    score = round(max(0.0, min(100.0, score)), 2)
    out['cme_confirmation_score'] = score
    out['cme_market_bias'] = 'long' if score >= 56 else 'short' if score <= 44 else 'neutral'
    out['source_cme_context'] = 'cmegroup'
    return out


def instrument_info(symbol=SYMBOL_PERP):
    items = req_bybit(URL_INSTRUMENTS, {'category': 'linear', 'symbol': symbol})['result']['list']
    if not items:
        return {}
    it = items[0]
    return {'funding_interval_min': it.get('fundingInterval'), 'launch_time_ms': it.get('launchTime'), 'price_scale': it.get('priceScale'), 'source_instrument_info': 'bybit'}


def _bybit_ticker(category, symbol):
    it = req_bybit(URL_TICKERS, {'category': category, 'symbol': symbol})['result']['list'][0]
    last = f(it.get('lastPrice')); bid1 = f(it.get('bid1Price')); ask1 = f(it.get('ask1Price')); low24 = f(it.get('lowPrice24h')); high24 = f(it.get('highPrice24h')); prev24 = f(it.get('prevPrice24h')); turnover24 = f(it.get('turnover24h')); volume24 = f(it.get('volume24h'))
    out = {'last': last, 'bid1': bid1, 'ask1': ask1, 'low24': low24, 'high24': high24, 'prev24': prev24, 'chg24_pct': pct(last, prev24), 'turnover24_usd': turnover24, 'volume24': volume24}
    if category == 'linear':
        mark = f(it.get('markPrice')); index = f(it.get('indexPrice')); funding = f(it.get('fundingRate'))
        spread_bps = ((ask1 - bid1) / ((ask1 + bid1) / 2.0)) * 10000 if None not in (bid1, ask1) and (ask1 + bid1) != 0 else None
        range_pos = (last - low24) / (high24 - low24) * 100.0 if None not in (last, low24, high24) and high24 != low24 else None
        out.update({'mark': mark, 'index': index, 'spread_bps': spread_bps, 'premium_vs_index_pct': pct(mark, index), 'funding_pct': None if funding is None else funding * 100.0, 'range_pos_pct': range_pos})
    return out


def _binance_spot_ticker(symbol):
    t24 = req_binance(BINANCE_URL_TICKER_24H, {'symbol': symbol})
    book = req_binance(BINANCE_URL_BOOK_TICKER, {'symbol': symbol})
    last = f(t24.get('lastPrice')); bid1 = f(book.get('bidPrice')); ask1 = f(book.get('askPrice')); low24 = f(t24.get('lowPrice')); high24 = f(t24.get('highPrice')); prev24 = f(t24.get('openPrice')); turnover24 = f(t24.get('quoteVolume')); volume24 = f(t24.get('volume'))
    return {'last': last, 'bid1': bid1, 'ask1': ask1, 'low24': low24, 'high24': high24, 'prev24': prev24, 'chg24_pct': pct(last, prev24), 'turnover24_usd': turnover24, 'volume24': volume24}


def ticker(category, symbol):
    if category == 'linear':
        return attach_source(_bybit_ticker(category, symbol), 'bybit', 'ticker')
    routed, source = resolve_route('spot_ticker', {'binance': lambda: _binance_spot_ticker(symbol), 'bybit': lambda: _bybit_ticker('spot', symbol)})
    return attach_source(routed, source, 'spot_ticker')


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
    mid = (bids[0][0] + asks[0][0]) / 2.0
    def depth_notional(side_rows, pct_band, side):
        total = 0.0; thr = mid * (1 - pct_band / 100.0) if side == 'bid' else mid * (1 + pct_band / 100.0)
        for p, q in side_rows:
            if (side == 'bid' and p >= thr) or (side == 'ask' and p <= thr): total += p * q
        return total
    def imbalance(pct_band):
        b = depth_notional(bids, pct_band, 'bid'); a = depth_notional(asks, pct_band, 'ask')
        if (a + b) == 0: return None
        return (b - a) / (b + a) * 100.0
    return {'orderbook_mid': mid, 'orderbook_imbalance_0_10_pct': imbalance(0.10), 'orderbook_imbalance_0_25_pct': imbalance(0.25), 'orderbook_imbalance_0_50_pct': imbalance(0.50), 'orderbook_imbalance_1_00_pct': imbalance(1.00), 'source_orderbook': 'bybit'}


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
    rows = req_binance(BINANCE_URL_KLINES, {'symbol': symbol, 'interval': interval, 'limit': limit})
    return [{'ts': int(r[0]), 'open': f(r[1]), 'high': f(r[2]), 'low': f(r[3]), 'close': f(r[4]), 'volume': f(r[5]), 'turnover': f(r[7])} for r in rows]


def median_turnover_ex_current(candles, lookback):
    vals = [c['turnover'] for c in candles[1:lookback+1] if c['turnover'] is not None]
    return median(vals) if vals else None


def median_range_ex_current(candles, lookback):
    vals = [(c['high'] - c['low']) for c in candles[1:lookback+1] if None not in (c['high'], c['low'])]
    return median(vals) if vals else None


def calc_atr(candles, period=14):
    if len(candles) < period + 1: return None
    trs = []
    for i in range(period):
        cur = candles[i]; prev = candles[i+1]
        if None in (cur['high'], cur['low'], prev['close']): continue
        trs.append(max(cur['high'] - cur['low'], abs(cur['high'] - prev['close']), abs(cur['low'] - prev['close'])))
    return sum(trs) / len(trs) if trs else None


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
    highs = [c['high'] for c in k5[:12] if c['high'] is not None]; lows = [c['low'] for c in k5[:12] if c['low'] is not None]
    vwap1 = calc_vwap(k5[:12]); vwap24 = calc_vwap(k60[:24]); last = cur5['close'] if cur5 else None
    out = {'turnover_5m_usd': t5, 'turnover_15m_usd': t15, 'turnover_1h_usd': t60, 'median_turnover_5m_usd': med5, 'median_turnover_15m_usd': med15, 'median_turnover_1h_usd': med60, 'volume_spike_5m_x': safe_div(t5, med5), 'volume_spike_15m_x': safe_div(t15, med15), 'volume_spike_1h_x': safe_div(t60, med60), 'range_5m': cur5r, 'range_15m': cur15r, 'median_range_5m': medr5, 'median_range_15m': medr15, 'range_expansion_5m_x': safe_div(cur5r, medr5), 'range_expansion_15m_x': safe_div(cur15r, medr15), 'prev_5m_high': prev5_high, 'prev_5m_low': prev5_low, 'prev_15m_high': prev15_high, 'prev_15m_low': prev15_low, 'swing_high_12x5m': max(highs) if highs else None, 'swing_low_12x5m': min(lows) if lows else None, 'vwap_1h': vwap1, 'vwap_24h': vwap24, 'price_vs_vwap_1h_pct': pct(last, vwap1), 'price_vs_vwap_24h_pct': pct(last, vwap24), 'atr_5m': calc_atr(k5, 14), 'atr_15m': calc_atr(k15, 14), 'price_low_2_5m': min([c['low'] for c in k5[:2] if c['low'] is not None], default=None), 'price_low_6_5m': min([c['low'] for c in k5[1:7] if c['low'] is not None], default=None), 'price_high_2_5m': max([c['high'] for c in k5[:2] if c['high'] is not None], default=None), 'price_high_6_5m': max([c['high'] for c in k5[1:7] if c['high'] is not None], default=None), 'vol_declining_5m': True if len(k5) > 2 and all(k5[i]['turnover'] <= k5[i+1]['turnover'] for i in range(0, 2) if None not in (k5[i]['turnover'], k5[i+1]['turnover'])) else False, 'source_structure': 'bybit'}
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
        'perp_spot_last_spread_pct': pct(perp.get('last'), spot.get('last')),
        'perp_5m_chg_pct': perp_chg,
        'spot_5m_chg_pct': spot_chg,
        'spot_perp_divergence_5m_pct_pt': None if None in (perp_chg, spot_chg) else (perp_chg - spot_chg),
        'source_perp_context': perp.get('source_ticker'),
        'source_spot_context': spot.get('source_spot_ticker'),
        'source_spot_klines': spot_kline_source,
    }


def liquidation_tracker_best_effort(duration_sec=8):
    out = {'liq_capture_window_sec': duration_sec, 'liq_available': False, 'long_liq_usd_5m': None, 'short_liq_usd_5m': None, 'liq_imbalance_pct': None}
    try: import websocket
    except Exception: return out
    collected = []
    def on_message(ws, message):
        try:
            data = json.loads(message); topic = data.get('topic', '')
            if 'liquidation' not in topic.lower(): return
            rows = data.get('data', [])
            if isinstance(rows, dict): rows = [rows]
            for r in rows:
                if r.get('symbol') != SYMBOL_PERP: continue
                side = fmt_side(r.get('side')); price = f(r.get('price'), 0.0); size = f(r.get('size'), 0.0); collected.append({'side': side, 'usd': price * size})
        except Exception: pass
    def on_open(ws):
        try: ws.send(json.dumps({'op': 'subscribe', 'args': ['liquidation.BTCUSDT', 'allLiquidation.BTCUSDT']}))
        except Exception: pass
    def runner():
        ws = websocket.WebSocketApp(WS_PUBLIC, on_open=on_open, on_message=on_message)
        try: ws.run_forever(sslopt={'cert_reqs': ssl.CERT_NONE}, ping_interval=20, ping_timeout=10)
        except Exception: pass
    t = threading.Thread(target=runner, daemon=True); t.start(); time.sleep(duration_sec)
    long_liq = short_liq = 0.0
    for x in collected:
        if x['side'] == 'Sell': long_liq += x['usd']
        elif x['side'] == 'Buy': short_liq += x['usd']
    total = long_liq + short_liq; imbalance = None if total == 0 else (short_liq - long_liq) / total * 100.0
    out.update({'liq_available': True, 'long_liq_usd_5m': long_liq, 'short_liq_usd_5m': short_liq, 'liq_imbalance_pct': imbalance, 'source_liquidations': 'bybit_ws'})
    return out


def global_data():
    d = SESSION.get(URL_GLOBAL, timeout=12).json()['data']
    return {'total_mcap_usd': f(d['total_market_cap']['usd']), 'btc_dom_pct': f(d['market_cap_percentage']['btc'])}
