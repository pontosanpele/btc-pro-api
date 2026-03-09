import json, os
from btc_pro_config import HISTORY_PATH, MAX_HISTORY_ROWS

def _hist_path(path): return os.path.expanduser(path)

def load_history(path=HISTORY_PATH, max_rows=MAX_HISTORY_ROWS):
    rows=[]; path=_hist_path(path)
    if not os.path.exists(path): return rows
    try:
        with open(path,'r',encoding='utf-8') as f:
            for line in f:
                line=line.strip()
                if not line: continue
                try: rows.append(json.loads(line))
                except Exception: continue
    except Exception:
        return []
    return rows[-max_rows:]

def append_history(snapshot, path=HISTORY_PATH, max_rows=MAX_HISTORY_ROWS):
    rows=load_history(path,max_rows); rows.append(snapshot); rows=rows[-max_rows:]
    path=_hist_path(path)
    try:
        with open(path,'w',encoding='utf-8') as f:
            for row in rows: f.write(json.dumps(row,ensure_ascii=False)+'\n')
    except Exception:
        pass

def compare_to_previous(snapshot, history_rows):
    btc=snapshot.get('btc',{}); prev=history_rows[-1] if history_rows else None; prev_btc=prev.get('btc',{}) if prev else {}
    def pv(k): return prev_btc.get(k)
    out={
        'history_rows_loaded':len(history_rows),'prev_trade_bias':pv('trade_bias'),'prev_market_regime':pv('market_regime'),
        'prev_breakout_quality_score':pv('breakout_quality_score'),'prev_bull_score':pv('bull_score'),'prev_bear_score':pv('bear_score'),
        'bull_score_change_1':None,'bear_score_change_1':None,'confidence_change_1':None,'breakout_quality_change_1':None,
        'cvd_change_window':None,'decision_stability_score':None,'signal_decay_score':None,'bias_persistence_count':0,
        'regime_persistence_count':0,'same_direction_streak':0,'bias_changed':False,'regime_changed':False,'quality_jump':False,
        'trap_risk_increased':False,'no_trade_activated':False,'trade_plan_invalidated':False,
    }
    if prev:
        for key, prevk in [('bull_score_change_1','bull_score'),('bear_score_change_1','bear_score'),('confidence_change_1','confidence_score'),('breakout_quality_change_1','breakout_quality_score')]:
            cur = btc.get(prevk); prv = pv(prevk)
            if cur is not None and prv is not None: out[key]=cur-prv
        if btc.get('cvd_trend_usd') is not None and pv('cvd_trend_usd') is not None: out['cvd_change_window']=btc['cvd_trend_usd']-pv('cvd_trend_usd')
        out['bias_changed']=btc.get('trade_bias') != pv('trade_bias')
        out['regime_changed']=btc.get('market_regime') != pv('market_regime')
        out['quality_jump']=(out['breakout_quality_change_1'] or 0) >= 15
        prev_trap=max(pv('bull_trap_risk') or 0, pv('bear_trap_risk') or 0)
        cur_trap=max(btc.get('bull_trap_risk') or 0, btc.get('bear_trap_risk') or 0)
        out['trap_risk_increased']=cur_trap-prev_trap >= 20
        out['no_trade_activated']=(pv('no_trade_active') is False) and (btc.get('no_trade_active') is True)
        out['trade_plan_invalidated']=pv('trade_plan_side') in ('long','short') and btc.get('trade_plan_side')=='no_trade'
    cur_bias=btc.get('trade_bias'); cur_regime=btc.get('market_regime'); cur_dir=btc.get('breakout_direction')
    for row in reversed(history_rows):
        if row.get('btc',{}).get('trade_bias')==cur_bias: out['bias_persistence_count'] += 1
        else: break
    for row in reversed(history_rows):
        if row.get('btc',{}).get('market_regime')==cur_regime: out['regime_persistence_count'] += 1
        else: break
    for row in reversed(history_rows):
        if row.get('btc',{}).get('breakout_direction')==cur_dir and cur_dir is not None: out['same_direction_streak'] += 1
        else: break
    conf=[]
    if out['confidence_change_1'] is not None: conf.append(abs(out['confidence_change_1']))
    if len(history_rows)>=2:
        p2=history_rows[-2].get('btc',{}); p1=history_rows[-1].get('btc',{})
        if p1.get('confidence_score') is not None and p2.get('confidence_score') is not None: conf.append(abs(p1['confidence_score']-p2['confidence_score']))
    avg=sum(conf)/len(conf) if conf else 0.0
    out['decision_stability_score']=round(max(0,100-avg*2.5),2)
    decay=0.0
    if out['confidence_change_1'] is not None and out['confidence_change_1']<0: decay += min(abs(out['confidence_change_1'])*2,40)
    if out['breakout_quality_change_1'] is not None and out['breakout_quality_change_1']<0: decay += min(abs(out['breakout_quality_change_1']),40)
    out['signal_decay_score']=round(min(decay,100),2)
    return out


def _btc_rows(history_rows):
    return [row.get("btc", {}) for row in history_rows if isinstance(row, dict)]

def rolling_values(history_rows, key, n=5):
    vals = []
    for row in _btc_rows(history_rows)[-n:]:
        v = row.get(key)
        if isinstance(v, (int, float)):
            vals.append(float(v))
    return vals

def rolling_mean(history_rows, key, n=5):
    vals = rolling_values(history_rows, key, n)
    return sum(vals) / len(vals) if vals else None

def rolling_median(history_rows, key, n=5):
    vals = sorted(rolling_values(history_rows, key, n))
    if not vals:
        return None
    m = len(vals) // 2
    return vals[m] if len(vals) % 2 else (vals[m-1] + vals[m]) / 2

def rolling_slope(history_rows, key, n=5):
    vals = rolling_values(history_rows, key, n)
    if len(vals) < 2:
        return None
    return (vals[-1] - vals[0]) / (len(vals) - 1)

def percentile_rank(history_rows, key, value, n=20):
    vals = rolling_values(history_rows, key, n)
    if not vals or value is None:
        return None
    below = sum(1 for x in vals if x <= value)
    return below / len(vals) * 100.0

def robust_zscore(history_rows, key, value, n=20):
    vals = rolling_values(history_rows, key, n)
    if len(vals) < 5 or value is None:
        return None
    vals = sorted(vals)
    mid = len(vals) // 2
    med = vals[mid] if len(vals) % 2 else (vals[mid-1] + vals[mid]) / 2
    abs_dev = sorted(abs(x - med) for x in vals)
    mad = abs_dev[mid] if len(abs_dev) % 2 else (abs_dev[mid-1] + abs_dev[mid]) / 2
    if mad == 0:
        return 0.0
    return 0.6745 * (value - med) / mad


def _in_band(value, center, band_pct):
    if value is None or center is None or center == 0:
        return False
    return abs(value - center) / center * 100.0 <= band_pct

def level_zone_memory(history_rows, center_price, side="support", band_pct=0.08, max_rows=30):
    rows = [row.get("btc", {}) for row in history_rows if isinstance(row, dict)][-max_rows:]
    if not rows or center_price in (None, 0):
        return {
            "zone_touch_count": 0,
            "zone_reject_count": 0,
            "zone_reclaim_count": 0,
            "zone_break_fail_count": 0,
            "zone_hold_count": 0,
            "zone_memory_score": 0.0,
            "zone_bounce_quality_avg": 0.0,
            "zone_bounce_quality_peak": 0.0,
            "zone_band_pct_used": 0.0,
            "zone_time_weighted_score": 0.0,
        }

    touch = reject = reclaim = break_fail = hold = 0
    prev_inside = False
    total = len(rows)
    weighted_score = 0.0
    weighted_sum = 0.0
    bounce_scores = []
    band_used_values = []

    for i, row in enumerate(rows):
        last = row.get("last")
        low = row.get("price_low_2_5m") or row.get("prev_5m_low") or last
        high = row.get("price_high_2_5m") or row.get("prev_5m_high") or last
        body = row.get("cur5m_body_pct_of_range") or 0.0

        adaptive_band = _adaptive_band_pct(row, center_price)
        band_pct_eff = max(band_pct, adaptive_band)
        band_used_values.append(band_pct_eff)

        inside = (
            _in_band(last, center_price, band_pct_eff)
            or _in_band(low, center_price, band_pct_eff)
            or _in_band(high, center_price, band_pct_eff)
        )

        event_score = 0.0
        if inside:
            touch += 1
            event_score += 8

        if side == "support":
            if low is not None and low <= center_price and last is not None and last >= center_price:
                reject += 1
                event_score += 14
                bounce_scores.append(_bounce_quality_score(row, center_price, side="support"))
            if prev_inside is False and last is not None and last >= center_price and inside:
                reclaim += 1
                event_score += 12
            if low is not None and low < center_price and last is not None and last > center_price:
                break_fail += 1
                event_score += 10
            if last is not None and last >= center_price and inside and body >= 20:
                hold += 1
                event_score += 8
        else:
            if high is not None and high >= center_price and last is not None and last <= center_price:
                reject += 1
                event_score += 14
                bounce_scores.append(_bounce_quality_score(row, center_price, side="resistance"))
            if prev_inside is False and last is not None and last <= center_price and inside:
                reclaim += 1
                event_score += 12
            if high is not None and high > center_price and last is not None and last < center_price:
                break_fail += 1
                event_score += 10
            if last is not None and last <= center_price and inside and body >= 20:
                hold += 1
                event_score += 8

        w = _time_decay_weight(i, total)
        weighted_sum += w
        weighted_score += event_score * w
        prev_inside = inside

    memory_score = min(weighted_score, 100.0)
    bounce_avg = sum(bounce_scores) / len(bounce_scores) if bounce_scores else 0.0
    bounce_peak = max(bounce_scores) if bounce_scores else 0.0
    band_avg = sum(band_used_values) / len(band_used_values) if band_used_values else 0.0

    return {
        "zone_touch_count": int(touch),
        "zone_reject_count": int(reject),
        "zone_reclaim_count": int(reclaim),
        "zone_break_fail_count": int(break_fail),
        "zone_hold_count": int(hold),
        "zone_memory_score": round(memory_score, 2),
        "zone_bounce_quality_avg": round(bounce_avg, 2),
        "zone_bounce_quality_peak": round(bounce_peak, 2),
        "zone_band_pct_used": round(band_avg, 4),
        "zone_time_weighted_score": round(weighted_score / weighted_sum, 2) if weighted_sum else 0.0,
    }

def _adaptive_band_pct(row, center_price, min_band_pct=0.05, max_band_pct=0.35):
    if center_price in (None, 0):
        return min_band_pct
    atr5 = row.get("atr_5m") or 0.0
    atr15 = row.get("atr_15m") or 0.0
    range5 = row.get("range_5m") or 0.0
    range15 = row.get("range_15m") or 0.0
    vol_anchor = max(atr5, atr15 * 0.7, range5 * 0.8, range15 * 0.35)
    if vol_anchor <= 0:
        return min_band_pct
    band_pct = (vol_anchor / center_price) * 100.0 * 0.55
    return max(min_band_pct, min(max_band_pct, band_pct))

def _time_decay_weight(idx_from_oldest, total):
    if total <= 1:
        return 1.0
    frac = idx_from_oldest / (total - 1)
    return 0.55 + 0.90 * frac

def _bounce_quality_score(row, center_price, side="support"):
    if center_price in (None, 0):
        return 0.0
    last = row.get("last")
    low = row.get("price_low_2_5m") or row.get("prev_5m_low") or last
    high = row.get("price_high_2_5m") or row.get("prev_5m_high") or last
    volume_quality = row.get("volume_quality_score") or 0.0
    vol5 = row.get("volume_spike_5m_x") or 0.0
    hold_bonus = 8.0 if (row.get("cur5m_body_pct_of_range") or 0.0) >= 20 else 0.0

    score = 0.0
    if side == "support":
        if low is not None and low < center_price and last is not None and last > center_price:
            bounce_pct = ((last - low) / center_price) * 100.0
            score += min(max(bounce_pct * 180, 0.0), 32.0)
        elif last is not None and last >= center_price:
            bounce_pct = ((last - center_price) / center_price) * 100.0
            score += min(max(bounce_pct * 120, 0.0), 18.0)
    else:
        if high is not None and high > center_price and last is not None and last < center_price:
            bounce_pct = ((high - last) / center_price) * 100.0
            score += min(max(bounce_pct * 180, 0.0), 32.0)
        elif last is not None and last <= center_price:
            bounce_pct = ((center_price - last) / center_price) * 100.0
            score += min(max(bounce_pct * 120, 0.0), 18.0)

    score += min(volume_quality * 0.18, 18.0)
    score += min(vol5 * 12.0, 14.0)
    score += hold_bonus
    return round(min(max(score, 0.0), 100.0), 2)
