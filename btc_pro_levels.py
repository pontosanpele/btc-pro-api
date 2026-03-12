import math


def liquidity_map_proxy(d):
    above = []
    below = []
    last = d.get('last')

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
            dist = abs(v - last)
            above.append((dist / weight, v, key))

    for key, weight in below_keys:
        v = d.get(key)
        if v is not None and last is not None and v < last:
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

    if ratio >= 3.0:
        major = 'ask'
    elif ratio >= 1.35:
        major = 'slight_ask'
    elif ratio <= 0.33:
        major = 'bid'
    elif ratio <= 0.74:
        major = 'slight_bid'
    else:
        major = 'balanced'

    if micro in ('ask', 'slight_ask') and major in ('ask', 'slight_ask'):
        wall_side = 'ask'
        explanation = 'near-book and major walls both lean ask'
    elif micro in ('bid', 'slight_bid') and major in ('bid', 'slight_bid'):
        wall_side = 'bid'
        explanation = 'near-book and major walls both lean bid'
    elif micro in ('ask', 'slight_ask') and major in ('bid', 'slight_bid'):
        wall_side = 'mixed'
        explanation = 'near-book ask pressure, but larger bid wall support below'
    elif micro in ('bid', 'slight_bid') and major in ('ask', 'slight_ask'):
        wall_side = 'mixed'
        explanation = 'near-book bid pressure, but larger ask wall overhead'
    elif micro == 'balanced' and major != 'balanced':
        wall_side = 'ask' if 'ask' in major else 'bid'
        explanation = f'near-book balanced, major walls lean {wall_side}'
    elif major == 'balanced' and micro != 'balanced':
        wall_side = 'ask' if 'ask' in micro else 'bid'
        explanation = f'near-book {wall_side} pressure dominates'
    else:
        wall_side = 'neutral'
        explanation = 'book and wall pressure broadly balanced'

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
    bull_trigger = d.get('liq_above_htf') or d.get('prev_1h_high') or d.get('liq_above_1') or d.get('prev_15m_high') or d.get('prev_5m_high')
    bear_trigger = d.get('liq_below_htf') or d.get('prev_1h_low') or d.get('liq_below_1') or d.get('prev_15m_low') or d.get('prev_5m_low')
    swing_high = d.get('swing_high_12x5m')
    swing_low = d.get('swing_low_12x5m')
    atr5 = d.get('atr_5m')
    atr15 = d.get('atr_15m')

    # Ne legyen túl közel egymáshoz a long/short belépő: 15m ATR + minimum %-os sáv.
    min_gap = None
    if last is not None:
        atr15_base = atr15 if atr15 is not None else 0.0
        min_gap = max(atr15_base * 0.95, last * 0.0048)

    if None not in (bull_trigger, bear_trigger, min_gap) and bull_trigger <= bear_trigger + min_gap:
        htf_up = d.get('liq_above_htf') or d.get('vwap_24h') or d.get('prev_15m_high')
        htf_dn = d.get('liq_below_htf') or d.get('vwap_1h') or d.get('prev_15m_low')

        if htf_up is not None and htf_up > bear_trigger:
            bull_trigger = max(bull_trigger, htf_up)
        if htf_dn is not None and htf_dn < bull_trigger:
            bear_trigger = min(bear_trigger, htf_dn)

        if None not in (bull_trigger, bear_trigger, min_gap) and bull_trigger <= bear_trigger + min_gap and last is not None:
            bull_trigger = max(bull_trigger, last + min_gap * 0.5)
            bear_trigger = min(bear_trigger, last - min_gap * 0.5)

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
            long_score += 10
        if delta > 0:
            long_score += min(delta * 0.18, 15)
        if cvd > 0:
            long_score += 10
        if ob > 0:
            long_score += min(ob * 0.8, 10)
        if nearest == 'above':
            long_score += 8
        if lower < 35:
            long_score += 6
        if body5 > 45:
            long_score += 6
    if None not in (last, bear_trigger) and last != 0:
        dist = abs(last - bear_trigger) / last * 100.0
        if dist <= 0.10:
            short_score += 35
        elif dist <= 0.20:
            short_score += 20
        elif dist <= 0.35:
            short_score += 10
        if p1 < 0.05:
            short_score += 10
        if delta < 0:
            short_score += min(abs(delta) * 0.18, 15)
        if cvd < 0:
            short_score += 10
        if ob < 0:
            short_score += min(abs(ob) * 0.8, 10)
        if nearest == 'below':
            short_score += 8
        if upper < 35:
            short_score += 6
        if body5 > 45:
            short_score += 6
    long_score = round(min(max(long_score, 0), 100), 2)
    short_score = round(min(max(short_score, 0), 100), 2)
    winner = 'none'
    long_ready = short_ready = False
    margin = abs(long_score - short_score)
    if long_score >= 45 and long_score >= short_score + 8:
        winner = 'long'; long_ready = True
    elif short_score >= 45 and short_score >= long_score + 8:
        winner = 'short'; short_ready = True
    return {
        'retest_long_score': long_score,
        'retest_short_score': short_score,
        'retest_winner_side': winner,
        'retest_long_ready': long_ready,
        'retest_short_ready': short_ready,
        'retest_quality_score': max(long_score, short_score),
        'retest_score_margin': round(margin, 2),
    }
