from btc_pro_config import clamp, NOISE_FLOOR, SESSION_PENALTY_PROFILE, SOFT_SCORE_WEIGHTS_V2, ORDERFLOW_CONFIDENCE_V2
from btc_pro_metrics import capped_log_score, bucket_score, regime_adaptive_weights
from btc_pro_history import level_zone_memory


def countertrend_penalty(d):
    dominant = d.get("dominant_bias_htf", "neutral")
    penalty_long = 0.0
    penalty_short = 0.0
    reasons = []

    if dominant == "long":
        penalty_short += 15.0
        reasons.append("htf_long_blocks_short")
        if (d.get("trend_15m") or "") != "down":
            penalty_short += 6.0
        if not d.get("bear_break_valid_v2"):
            penalty_short += 6.0
    elif dominant == "short":
        penalty_long += 15.0
        reasons.append("htf_short_blocks_long")
        if (d.get("trend_15m") or "") != "up":
            penalty_long += 6.0
        if not d.get("bull_break_valid_v2"):
            penalty_long += 6.0

    return {
        "countertrend_penalty_long": round(clamp(penalty_long, 0, 40), 2),
        "countertrend_penalty_short": round(clamp(penalty_short, 0, 40), 2),
        "countertrend_penalty_reasons": reasons[:3],
    }

def breakout_quality(d):
    score=0.0; vol5=d.get('volume_spike_5m_x') or 0.0; vol15=d.get('volume_spike_15m_x') or 0.0; re5=d.get('range_expansion_5m_x') or 0.0; delta=abs(d.get('recent_notional_delta_pct') or 0.0); body=d.get('cur5m_body_pct_of_range') or 0.0
    if vol5>0.8: score += min((vol5-0.8)*18,20)
    if vol15>0.8: score += min((vol15-0.8)*14,18)
    if re5>0.9: score += min((re5-0.9)*16,16)
    score += min(delta*0.28,18); score += min(body*0.15,12)
    if abs(d.get('spot_perp_divergence_5m_pct_pt') or 0.0)<0.05: score += 5
    if abs(d.get('cvd_trend_usd') or 0.0)>50000: score += min(abs(d.get('cvd_trend_usd') or 0.0)/100000.0,11)
    direction='up' if (d.get('recent_notional_delta_pct') or 0)>8 else 'down' if (d.get('recent_notional_delta_pct') or 0)<-8 else 'neutral'
    return {'breakout_quality_score':round(clamp(score,0,100),2),'breakout_direction':direction}

def breakout_validator(d):
    last=d.get('last'); bull_trigger=d.get('bull_trigger_price'); bear_trigger=d.get('bear_trigger_price'); vol5=d.get('volume_spike_5m_x') or 0.0; body=d.get('cur5m_body_pct_of_range') or 0.0; delta=d.get('recent_notional_delta_pct') or 0.0; upper=d.get('cur5m_upper_wick_pct_of_range') or 0.0; lower=d.get('cur5m_lower_wick_pct_of_range') or 0.0; bull_valid=bear_valid=False; strength=0.0
    if None not in (last,bull_trigger) and last>bull_trigger and vol5>0.7 and body>60 and delta>8 and upper<35: bull_valid=True; strength += 45
    if None not in (last,bear_trigger) and last<bear_trigger and vol5>0.7 and body>60 and delta<-8 and lower<35: bear_valid=True; strength += 45
    if d.get('multi_tf_alignment_score') is not None: strength += min(abs(d['multi_tf_alignment_score'])*5,15)
    return {'bull_break_valid':bull_valid,'bear_break_valid':bear_valid,'reclaim_strength_score':round(clamp(strength,0,100),2)}

def rr_metrics(d):
    last=d.get('last'); out={'rr_long_to_t1':None,'rr_long_to_t2':None,'rr_short_to_t1':None,'rr_short_to_t2':None,'stop_quality':'unknown','entry_quality_long':0,'entry_quality_short':0}
    if last is None: return out
    long_stop,short_stop=d.get('atr_stop_long'),d.get('atr_stop_short'); t1l,t2l,t1s,t2s=d.get('target_long_1'),d.get('target_long_2'),d.get('target_short_1'),d.get('target_short_2')
    if None not in (long_stop,t1l) and (last-long_stop)>0: out['rr_long_to_t1']=(t1l-last)/(last-long_stop)
    if None not in (long_stop,t2l) and (last-long_stop)>0: out['rr_long_to_t2']=(t2l-last)/(last-long_stop)
    if None not in (short_stop,t1s) and (short_stop-last)>0: out['rr_short_to_t1']=(last-t1s)/(short_stop-last)
    if None not in (short_stop,t2s) and (short_stop-last)>0: out['rr_short_to_t2']=(last-t2s)/(short_stop-last)
    best=max([x for x in [out['rr_long_to_t1'],out['rr_long_to_t2'],out['rr_short_to_t1'],out['rr_short_to_t2']] if x is not None],default=None)
    if best is not None: out['stop_quality']='good' if best>=2.0 else 'ok' if best>=1.2 else 'poor'
    return out

def trap_detector(d):
    bull=bear=0.0; vol5=d.get('volume_spike_5m_x') or 0.0; cvd=d.get('cvd_trend_usd') or 0.0; last=d.get('last'); bull_trigger=d.get('bull_trigger_price'); bear_trigger=d.get('bear_trigger_price'); upper=d.get('cur5m_upper_wick_pct_of_range') or 0.0; lower=d.get('cur5m_lower_wick_pct_of_range') or 0.0
    if None not in (last,bull_trigger) and last>bull_trigger and vol5<0.5 and cvd<0 and upper>30: bull += 70
    if None not in (last,bear_trigger) and last<bear_trigger and vol5<0.5 and cvd>0 and lower>30: bear += 70
    bull=round(clamp(bull,0,100),2); bear=round(clamp(bear,0,100),2); alert='bull_trap_risk' if bull>=60 and bull>bear else 'bear_trap_risk' if bear>=60 and bear>bull else 'none'
    return {'bull_trap_risk':bull,'bear_trap_risk':bear,'trap_alert':alert}

def exhaustion_detector(d):
    up=down=0.0; vol5=d.get('volume_spike_5m_x') or 0.0; vol15=d.get('volume_spike_15m_x') or 0.0; upper=d.get('cur5m_upper_wick_pct_of_range') or 0.0; lower=d.get('cur5m_lower_wick_pct_of_range') or 0.0; p1=d.get('price_vs_vwap_1h_pct') or 0.0; delta=abs(d.get('recent_notional_delta_pct') or 0.0)
    if p1>0 and upper>40: up += 30
    if p1<0 and lower>40: down += 30
    if vol5<0.7 and vol15<0.8: up += 15; down += 15
    if delta<5: up += 10; down += 10
    return {'move_exhaustion_up':round(clamp(up,0,100),2),'move_exhaustion_down':round(clamp(down,0,100),2)}

def rejection_detector(d):
    last=d.get('last'); bull_trigger=d.get('bull_trigger_price'); bear_trigger=d.get('bear_trigger_price'); upper=d.get('cur5m_upper_wick_pct_of_range') or 0.0; lower=d.get('cur5m_lower_wick_pct_of_range') or 0.0; body=d.get('cur5m_body_pct_of_range') or 0.0
    long_rejection = None not in (last,bull_trigger) and last < bull_trigger and upper > 45 and body < 45
    short_rejection = None not in (last,bear_trigger) and last > bear_trigger and lower > 45 and body < 45
    return {'long_rejection_detected':long_rejection,'short_rejection_detected':short_rejection}

def orderflow_consistency(d):
    bullish_votes=bearish_votes=0.0; delta=d.get('recent_notional_delta_pct') or 0.0; cvd=d.get('cvd_trend_usd') or 0.0; taker=d.get('recent_taker_buy_ratio_pct') or 50.0; spot_div=d.get('spot_perp_divergence_5m_pct_pt') or 0.0; large_buy=d.get('large_100k_buy_usd') or 0.0; large_sell=d.get('large_100k_sell_usd') or 0.0
    if delta>5: bullish_votes += 1
    elif delta<-5: bearish_votes += 1
    if cvd>0: bullish_votes += 1
    elif cvd<0: bearish_votes += 1
    if taker>53: bullish_votes += 1
    elif taker<47: bearish_votes += 1
    if spot_div>-0.02: bullish_votes += 0.5
    if spot_div<0.02: bearish_votes += 0.5
    if large_buy>large_sell: bullish_votes += 1
    elif large_sell>large_buy: bearish_votes += 1
    diff=bullish_votes-bearish_votes; score=50+diff*12.5; side='neutral'
    if diff>=1.5: side='bullish'
    elif diff<=-1.5: side='bearish'
    return {'orderflow_consistency_score':round(min(max(score,0),100),2),'orderflow_consistency_side':side}

def execution_realism(d):
    last=d.get('last'); spread_bps=d.get('spread_bps'); side=d.get('trade_bias'); ref=d.get('bull_trigger_price') if side=='long' else d.get('bear_trigger_price') if side=='short' else None
    trigger_distance_pct=abs(ref-last)/last*100.0 if None not in (last,ref) and last!=0 else None; slippage=0.0
    if spread_bps is not None: slippage += min(spread_bps*1.5,30)
    if d.get('session_name')=='weekend': slippage += 15
    late=0.0
    if trigger_distance_pct is not None: late += min(trigger_distance_pct*60,45)
    p1=d.get('price_vs_vwap_1h_pct')
    if p1 is not None and abs(p1)>0.35: late += 20
    slippage=round(clamp(slippage,0,100),2); late=round(clamp(late,0,100),2)
    return {'trigger_distance_pct':trigger_distance_pct,'slippage_risk':slippage,'late_entry_risk':late,'execution_quality_score':round(clamp(100-(slippage*0.5+late*0.6),0,100),2)}

def entry_timing_score(d):
    score=50.0
    if d.get('above_long_trigger_acceptance') or d.get('below_short_trigger_acceptance'): score += 15
    if d.get('bull_break_valid') or d.get('bear_break_valid'): score += 20
    if d.get('retest_winner_side') != 'none': score += 10
    score += min((d.get('volume_quality_score') or 0)*0.12,10)
    score += min((d.get('orderflow_consistency_score') or 0)*0.08,8)
    score -= min((d.get('slippage_risk') or 0)*0.25,20)
    score -= min((d.get('late_entry_risk') or 0)*0.30,20)
    if d.get('long_rejection_detected') or d.get('short_rejection_detected'): score -= 18
    return {'entry_timing_score':round(min(max(score,0),100),2)}

def no_trade_filter(d):
    score = 0.0
    reasons = []

    vol5 = d.get("volume_spike_5m_x") or 0.0
    vol15 = d.get("volume_spike_15m_x") or 0.0
    confidence = d.get("confidence_score") or 0.0
    regime = d.get("market_regime")
    dominant = d.get("dominant_bias_htf", "neutral")

    if vol5 < 0.5:
        score += 20
        reasons.append("very_low_5m_volume")
    if vol15 < 0.6:
        score += 15
        reasons.append("weak_15m_volume")
    if confidence < 10:
        score += 20
        reasons.append("low_signal_confidence")
    if d.get("session_name") == "weekend":
        score += 10
        reasons.append("weekend_thin_liquidity")
    if regime == "low_liquidity_range":
        score += 15
        reasons.append("range_chop_regime")
    if d.get("trap_alert") != "none":
        score += 15
        reasons.append("trap_risk")

    ctp_long = d.get("countertrend_penalty_long") or 0.0
    ctp_short = d.get("countertrend_penalty_short") or 0.0
    if max(ctp_long, ctp_short) >= 15:
        score += 8
        reasons.append("countertrend_context")

    override = 0.0
    override_reasons = []
    direction_score = d.get("direction_consensus_score") or 0.0
    retest = d.get("retest_quality_score") or 0.0
    execution = d.get("execution_feasibility_score") or 0.0
    expected_value = d.get("expected_value_score") or 0.0
    trigger_behavior = max(d.get("trigger_behavior_long_score") or 0.0, d.get("trigger_behavior_short_score") or 0.0)

    context_friendly = regime in ("trend_build_long", "trend_build_short", "short_squeeze", "long_flush", "impulse_up", "impulse_down")
    if direction_score >= 80:
        override += 8
        override_reasons.append("strong_direction_consensus")
    if retest >= 70:
        override += 8
        override_reasons.append("strong_retest_quality")
    if execution >= 75:
        override += 6
        override_reasons.append("good_execution_feasibility")
    if expected_value >= 70:
        override += 8
        override_reasons.append("positive_expected_value")
    if trigger_behavior >= 55:
        override += 5
        override_reasons.append("strong_trigger_behavior")
    if context_friendly:
        override += 6
        override_reasons.append("regime_context_override")

    if regime == "low_liquidity_range":
        strong_override = (
            direction_score >= 85
            and retest >= 75
            and (d.get("volume_spike_15m_x") or 0.0) >= 1.0
            and (d.get("breakout_quality_v2") or 0.0) >= 55
        )
        if not strong_override:
            override = min(override, 10.0)
        else:
            override = min(override + 4.0, 18.0)
            override_reasons.append("range_override_confirmed")
    else:
        override = min(override, 26.0)

    if dominant == "neutral" and regime == "low_liquidity_range":
        score += 6
        reasons.append("neutral_htf_in_range")

    adjusted_score = clamp(score - override, 0, 100)
    active = adjusted_score >= 45

    return {
        "no_trade_score": round(adjusted_score, 2),
        "no_trade_reason": reasons,
        "no_trade_active": active,
        "no_trade_raw_score": round(clamp(score, 0, 100), 2),
        "no_trade_override_score": round(override, 2),
        "no_trade_override_active": override >= 12.0,
        "no_trade_override_reason": override_reasons[:4],
    }

def classify_no_trade_context(d):
    if not d.get('no_trade_active'): return {'no_trade_context':'none'}
    if d.get('trap_alert')!='none': return {'no_trade_context':'no_trade_due_to_trap_risk'}
    if (d.get('confidence_score') or 0)<10: return {'no_trade_context':'no_trade_due_to_low_confidence'}
    if d.get('market_regime')=='low_liquidity_range': return {'no_trade_context':'no_trade_due_to_chop'}
    return {'no_trade_context':'no_trade_due_to_late_entry'}


def confidence_decomposition(d):
    structure = 0.0
    orderflow = 0.0
    volume = 0.0
    regime_fit = 0.0
    execution = 0.0

    structure += min((d.get("breakout_quality_score") or 0) * 0.5, 30)
    structure += 15 if d.get("bull_break_valid") or d.get("bear_break_valid") else 0
    structure += 12 if d.get("retest_winner_side") != "none" else 0
    structure += min(abs(d.get("direction_consensus_score") or 0) * 0.15, 18)

    delta_w = ORDERFLOW_CONFIDENCE_V2["delta_weight"]
    of_w = ORDERFLOW_CONFIDENCE_V2["orderflow_consistency_weight"]
    ds_w = ORDERFLOW_CONFIDENCE_V2["delta_strength_weight"]
    fa_w = ORDERFLOW_CONFIDENCE_V2["flow_alignment_weight"]

    orderflow += min(abs(d.get("recent_notional_delta_pct") or 0) * (22 * delta_w), 28)
    orderflow += min(abs((d.get("orderflow_consistency_score") or 50) - 50) * (1.6 * of_w), 24)
    orderflow += min(abs(d.get("delta_strength_score") or 0) * ds_w, 18)
    orderflow += min((d.get("flow_alignment_score") or 0) * fa_w, 18)

    volume += min((d.get("volume_quality_score") or 0) * 0.6, 45)
    volume += min((d.get("volume_spike_15m_x") or 0) * 18, 25)
    volume += 10 if (d.get("range_expansion_15m_x") or 0) > 0.9 else 0

    regime = d.get("market_regime")
    if regime in ("trend_build_long", "trend_build_short", "impulse_up", "impulse_down"):
        regime_fit += 30
    elif regime in ("neutral",):
        regime_fit += 18
    elif regime in ("low_liquidity_range",):
        regime_fit += 8
    else:
        regime_fit += 15
    regime_fit += min(abs(d.get("multi_tf_alignment_score") or 0) * 10, 25)

    execution += 35
    execution += min((d.get("execution_quality_score") or 0) * 0.35, 35)
    execution += min((d.get("entry_timing_score") or 0) * 0.25, 20)
    if d.get("long_rejection_detected") or d.get("short_rejection_detected"):
        execution -= 18

    out = {
        "confidence_structure": round(min(max(structure, 0), 100), 2),
        "confidence_orderflow": round(min(max(orderflow, 0), 100), 2),
        "confidence_volume": round(min(max(volume, 0), 100), 2),
        "confidence_regime_fit": round(min(max(regime_fit, 0), 100), 2),
        "confidence_execution": round(min(max(execution, 0), 100), 2),
    }
    out["confidence_composite_v2"] = round(
        out["confidence_structure"] * 0.26
        + out["confidence_orderflow"] * 0.22
        + out["confidence_volume"] * 0.16
        + out["confidence_regime_fit"] * 0.16
        + out["confidence_execution"] * 0.20,
        2,
    )
    return out

def execution_feasibility_score(d):
    score = 100.0
    score -= min((d.get("slippage_risk") or 0) * 0.6, 28)
    score -= min((d.get("late_entry_risk") or 0) * 0.7, 28)

    if d.get("session_name") == "weekend":
        score -= 10
    if (d.get("spread_bps") or 0) > 2.0:
        score -= 10
    if d.get("long_rejection_detected") or d.get("short_rejection_detected"):
        score -= 12
    if d.get("no_trade_active"):
        score -= 18

    return {"execution_feasibility_score": round(min(max(score, 0), 100), 2)}

def expected_value_score(d):
    rr_candidates = [
        d.get("rr_long_to_t1"),
        d.get("rr_long_to_t2"),
        d.get("rr_short_to_t1"),
        d.get("rr_short_to_t2"),
    ]
    rr_candidates = [x for x in rr_candidates if isinstance(x, (int, float))]
    best_rr = max(rr_candidates) if rr_candidates else 0.0

    setup = d.get("setup_readiness_score") or 0.0
    timing = d.get("entry_timing_score") or 0.0
    execution = d.get("execution_feasibility_score") or 0.0
    conflict_penalty = (d.get("signal_conflict_score") or 0.0) * 0.35
    trap_penalty = max(d.get("bull_trap_risk") or 0, d.get("bear_trap_risk") or 0) * 0.20
    no_trade_penalty = (d.get("no_trade_score") or 0.0) * 0.25

    rr_component = min(best_rr * 18, 45)
    score = rr_component + setup * 0.28 + timing * 0.18 + execution * 0.16 - conflict_penalty - trap_penalty - no_trade_penalty
    return {
        "best_rr_candidate": round(best_rr, 4) if rr_candidates else None,
        "expected_value_score": round(min(max(score, 0), 100), 2),
    }

def decision_drivers(d):
    positives = []
    negatives = []

    checks_pos = [
        (d.get("direction_consensus_side") == "long", "strong_long_consensus"),
        (d.get("direction_consensus_side") == "short", "strong_short_consensus"),
        ((d.get("retest_winner_side") == "long"), "long_retest_winner"),
        ((d.get("retest_winner_side") == "short"), "short_retest_winner"),
        ((d.get("entry_timing_score") or 0) >= 60, "good_entry_timing"),
        ((d.get("execution_feasibility_score") or 0) >= 65, "good_execution_feasibility"),
        ((d.get("expected_value_score") or 0) >= 55, "positive_expected_value"),
        ((d.get("bull_break_valid") is True), "bull_break_valid"),
        ((d.get("bear_break_valid") is True), "bear_break_valid"),
        ((d.get("quality_jump") is True), "quality_improving"),
    ]
    checks_neg = [
        ((d.get("no_trade_active") is True), "no_trade_filter_active"),
        ((d.get("signal_conflict_score") or 0) >= 55, "high_signal_conflict"),
        ((d.get("orderflow_consistency_side") == "bearish" and d.get("direction_consensus_side") == "long"), "bearish_orderflow_vs_long_setup"),
        ((d.get("orderflow_consistency_side") == "bullish" and d.get("direction_consensus_side") == "short"), "bullish_orderflow_vs_short_setup"),
        (((d.get("volume_spike_5m_x") or 0) < 0.5), "weak_5m_volume"),
        (((d.get("volume_spike_15m_x") or 0) < 0.6), "weak_15m_volume"),
        (((d.get("execution_feasibility_score") or 0) < 50), "poor_execution_feasibility"),
        (((d.get("expected_value_score") or 0) < 40), "low_expected_value"),
        ((d.get("long_rejection_detected") is True), "long_rejection_detected"),
        ((d.get("short_rejection_detected") is True), "short_rejection_detected"),
    ]

    for cond, label in checks_pos:
        if cond:
            positives.append(label)
    for cond, label in checks_neg:
        if cond:
            negatives.append(label)

    return {
        "decision_drivers_positive": positives[:4],
        "decision_drivers_negative": negatives[:4],
    }


def context_penalty_score(d):
    penalty = 0.0
    reasons = []

    session = d.get("session_name") or "weekend"
    prof = SESSION_PENALTY_PROFILE.get(session, SESSION_PENALTY_PROFILE["weekend"])
    penalty += prof["base_penalty"]
    if prof["base_penalty"] > 0:
        reasons.append(f"{session}_liquidity_penalty")

    if d.get("market_regime") == "low_liquidity_range":
        penalty += 12
        reasons.append("low_liquidity_range")
    elif d.get("market_regime") in ("neutral",):
        penalty += 4

    vol5 = d.get("volume_spike_5m_x") or 0.0
    vol15 = d.get("volume_spike_15m_x") or 0.0
    if vol5 < prof["vol5_warn"]:
        penalty += 10
        reasons.append("weak_5m_volume")
    elif vol5 < prof["vol5_warn"] * 1.35:
        penalty += 4

    if vol15 < prof["vol15_warn"]:
        penalty += 10
        reasons.append("weak_15m_volume")
    elif vol15 < prof["vol15_warn"] * 1.30:
        penalty += 4

    if (d.get("spread_bps") or 0) > 2.0:
        penalty += 8
        reasons.append("wide_spread")

    if (d.get("signal_conflict_score") or 0) >= 55:
        penalty += 18
        reasons.append("signal_conflict")

    td = d.get("trigger_distance_pct")
    if td is not None and td < 0.01:
        penalty += 4
        reasons.append("trigger_too_close_noise")
    elif td is not None and td > 0.35:
        penalty += 8
        reasons.append("trigger_too_far")

    wick_noise = max(d.get("cur5m_upper_wick_pct_of_range") or 0, d.get("cur5m_lower_wick_pct_of_range") or 0)
    if wick_noise > 80:
        penalty += 8
        reasons.append("wick_noise")
    elif wick_noise > 60:
        penalty += 4
        reasons.append("wick_noise")

    return {
        "context_penalty_score": round(min(max(penalty, 0), 100), 2),
        "context_penalty_reasons": reasons[:4],
    }

def invalidation_quality(d):
    last = d.get("last")
    side = d.get("trade_bias")
    atr = d.get("atr_5m")
    long_inv = d.get("invalidation_long")
    short_inv = d.get("invalidation_short")
    liq_above = d.get("liq_above_1")
    liq_below = d.get("liq_below_1")

    score = 50.0
    logic = "unknown"
    structural = False

    if side == "long" and None not in (last, long_inv):
        dist = abs(last - long_inv)
        if atr not in (None, 0):
            atr_mult = dist / atr
            if 0.8 <= atr_mult <= 2.5:
                score += 20
            elif atr_mult < 0.5:
                score -= 18
            elif atr_mult > 3.2:
                score -= 10
        if liq_below is not None and long_inv <= liq_below:
            score += 12
            structural = True
            logic = "below_support_liquidity"
        else:
            logic = "inside_structure"

    elif side == "short" and None not in (last, short_inv):
        dist = abs(short_inv - last)
        if atr not in (None, 0):
            atr_mult = dist / atr
            if 0.8 <= atr_mult <= 2.5:
                score += 20
            elif atr_mult < 0.5:
                score -= 18
            elif atr_mult > 3.2:
                score -= 10
        if liq_above is not None and short_inv >= liq_above:
            score += 12
            structural = True
            logic = "above_resistance_liquidity"
        else:
            logic = "inside_structure"

    if d.get("long_rejection_detected") or d.get("short_rejection_detected"):
        score -= 8

    return {
        "invalidation_quality_score": round(min(max(score, 0), 100), 2),
        "stop_location_logic": logic,
        "structural_invalidation_valid": structural,
    }


def regime_weighted_composite(d):
    w = regime_adaptive_weights(d.get("market_regime"))
    flow = d.get("confidence_orderflow") or 0.0
    vol = d.get("confidence_volume") or 0.0
    struct = d.get("confidence_structure") or 0.0
    execution = d.get("confidence_execution") or 0.0
    context = max(0.0, 100 - (d.get("context_penalty_score") or 0.0))
    score = flow*w["flow"] + vol*w["volume"] + struct*w["structure"] + execution*w["execution"] + context*w["context"]
    return {"confidence_regime_weighted": round(score, 2)}

def recalibrated_breakout_quality(d):
    vol5 = capped_log_score(d.get("volume_spike_5m_x"), low=0.75, high=1.8, max_score=30) or 0.0
    vol15 = capped_log_score(d.get("volume_spike_15m_x"), low=0.70, high=1.8, max_score=24) or 0.0
    body = bucket_score(d.get("cur5m_body_pct_of_range") or 0.0, [(35, 8), (50, 14), (65, 20), (80, 24)]) or 0.0
    delta = d.get("delta_pct_nf")
    delta_score = bucket_score(abs(delta or 0.0), [(4, 4), (8, 10), (15, 16), (25, 22), (40, 28)]) or 0.0
    cluster = (d.get("breakout_confirmation_cluster") or 0.0) * 0.25
    noise_penalty = 0.0
    if abs(d.get("spot_perp_div_nf") or 0.0) > 0.06:
        noise_penalty += 6
    score = vol5 + vol15 + body + delta_score + cluster - noise_penalty
    return {"breakout_quality_v2": round(clamp(score, 0, 100), 2)}

def recalibrated_setup_readiness(d):
    score = (
        (d.get("confidence_regime_weighted") or 0.0) * 0.26
        + (d.get("expected_value_score") or 0.0) * 0.20
        + (d.get("entry_timing_score") or 0.0) * 0.14
        + (d.get("flow_alignment_score") or 0.0) * 0.12
        + (d.get("retest_support_cluster") or 0.0) * 0.12
        + (d.get("invalidation_quality_score") or 0.0) * 0.10
        - (d.get("context_penalty_score") or 0.0) * 0.10
        - (d.get("signal_conflict_score") or 0.0) * 0.08
    )
    return {"setup_readiness_v2": round(clamp(score, 0, 100), 2)}


def trigger_requirements(d):
    long_missing = []
    short_missing = []

    if not d.get("above_long_trigger_acceptance"):
        long_missing.append("need_long_acceptance")
    if not d.get("bull_break_valid"):
        long_missing.append("need_bull_break_valid")
    if (d.get("volume_spike_15m_x") or 0) < 0.8:
        long_missing.append("need_stronger_15m_volume")
    if (d.get("signal_conflict_score") or 0) >= 25:
        long_missing.append("need_lower_conflict")
    if (d.get("execution_feasibility_score") or 0) < 55:
        long_missing.append("need_better_execution")
    if (d.get("context_penalty_score") or 0) >= 28:
        long_missing.append("need_better_context")

    if not d.get("below_short_trigger_acceptance"):
        short_missing.append("need_short_acceptance")
    if not d.get("bear_break_valid"):
        short_missing.append("need_bear_break_valid")
    if (d.get("volume_spike_15m_x") or 0) < 0.8:
        short_missing.append("need_stronger_15m_volume")
    if (d.get("signal_conflict_score") or 0) >= 25:
        short_missing.append("need_lower_conflict")
    if (d.get("execution_feasibility_score") or 0) < 55:
        short_missing.append("need_better_execution")
    if (d.get("context_penalty_score") or 0) >= 28:
        short_missing.append("need_better_context")

    return {
        "missing_for_long_trigger": long_missing[:6],
        "missing_for_short_trigger": short_missing[:6],
    }

def trigger_behavior_score(d):
    last = d.get("last")
    bull = d.get("bull_trigger_price")
    bear = d.get("bear_trigger_price")
    body = d.get("cur5m_body_pct_of_range") or 0.0
    upper = d.get("cur5m_upper_wick_pct_of_range") or 0.0
    lower = d.get("cur5m_lower_wick_pct_of_range") or 0.0
    delta = d.get("recent_notional_delta_pct") or 0.0
    close_bias = d.get("perp_5m_chg_pct") or 0.0

    long_score = 0.0
    short_score = 0.0

    if None not in (last, bull) and last != 0:
        dist = abs(last - bull) / last * 100.0
        if dist <= 0.05:
            long_score += 28
        elif dist <= 0.12:
            long_score += 18
        elif dist <= 0.25:
            long_score += 8
        if body > 45:
            long_score += 12
        elif body > 30:
            long_score += 6
        if upper < 35:
            long_score += 8
        if delta > 6:
            long_score += 10
        if close_bias > 0:
            long_score += 6
        if d.get("above_long_trigger_acceptance"):
            long_score += 14
        if d.get("long_rejection_detected"):
            long_score -= 14

    if None not in (last, bear) and last != 0:
        dist = abs(last - bear) / last * 100.0
        if dist <= 0.05:
            short_score += 28
        elif dist <= 0.12:
            short_score += 18
        elif dist <= 0.25:
            short_score += 8
        if body > 45:
            short_score += 12
        elif body > 30:
            short_score += 6
        if lower < 35:
            short_score += 8
        if delta < -6:
            short_score += 10
        if close_bias < 0:
            short_score += 6
        if d.get("below_short_trigger_acceptance"):
            short_score += 14
        if d.get("short_rejection_detected"):
            short_score -= 14

    return {
        "trigger_behavior_long_score": round(min(max(long_score, 0), 100), 2),
        "trigger_behavior_short_score": round(min(max(short_score, 0), 100), 2),
    }

def signal_freshness_score(d):
    freshness = 70.0
    decay = d.get("signal_decay_score") or 0.0
    if decay > 0:
        freshness -= min(decay * 0.9, 35)

    if d.get("quality_jump"):
        freshness += 12
    if d.get("bias_changed"):
        freshness += 6
    if d.get("regime_changed"):
        freshness += 4

    return {"signal_freshness_score": round(min(max(freshness, 0), 100), 2)}

def level_fatigue_score(d):
    long_tests = (d.get("same_direction_streak") or 0) + (1 if d.get("nearest_liquidity_side") == "above" else 0)
    short_tests = (d.get("same_direction_streak") or 0) + (1 if d.get("nearest_liquidity_side") == "below" else 0)

    long_fatigue = min(long_tests * 8, 40)
    short_fatigue = min(short_tests * 8, 40)

    return {
        "long_level_test_count": long_tests,
        "short_level_test_count": short_tests,
        "long_level_fatigue_score": round(long_fatigue, 2),
        "short_level_fatigue_score": round(short_fatigue, 2),
    }



def zone_proximity_features(d):
    last = d.get("last")
    out = {
        "distance_to_support_pct": None,
        "distance_to_resistance_pct": None,
        "zone_position_in_support": None,
        "zone_position_in_resistance": None,
        "nearest_zone_side": "none",
        "zone_proximity_long_score": 0.0,
        "zone_proximity_short_score": 0.0,
    }
    if last in (None, 0):
        return out

    s_low = d.get("support_zone_low")
    s_high = d.get("support_zone_high")
    r_low = d.get("resistance_zone_low")
    r_high = d.get("resistance_zone_high")

    in_support = False
    in_resistance = False

    def _near_score(dist):
        if dist <= 0.03:
            return 92.0
        if dist <= 0.06:
            return 84.0
        if dist <= 0.10:
            return 72.0
        if dist <= 0.16:
            return 58.0
        if dist <= 0.30:
            return 34.0
        return 0.0

    if s_low is not None and s_high is not None:
        if s_low <= last <= s_high and s_high > s_low:
            in_support = True
            pos = (last - s_low) / (s_high - s_low)
            out["zone_position_in_support"] = round(pos, 4)
            out["distance_to_support_pct"] = 0.0
            out["zone_proximity_long_score"] = round(max(0.0, 96.0 - pos * 72.0), 2)
        else:
            edge = s_high if last > s_high else s_low
            dist = abs(last - edge) / last * 100.0
            out["distance_to_support_pct"] = round(dist, 4)
            out["zone_proximity_long_score"] = _near_score(dist)

    if r_low is not None and r_high is not None:
        if r_low <= last <= r_high and r_high > r_low:
            in_resistance = True
            pos = (last - r_low) / (r_high - r_low)
            out["zone_position_in_resistance"] = round(pos, 4)
            out["distance_to_resistance_pct"] = 0.0
            out["zone_proximity_short_score"] = round(max(0.0, 20.0 + pos * 76.0), 2)
        else:
            edge = r_low if last < r_low else r_high
            dist = abs(last - edge) / last * 100.0
            out["distance_to_resistance_pct"] = round(dist, 4)
            out["zone_proximity_short_score"] = _near_score(dist)

    ds = out["distance_to_support_pct"]
    dr = out["distance_to_resistance_pct"]
    if ds is not None and dr is not None:
        out["nearest_zone_side"] = "support" if ds <= dr else "resistance"
    elif ds is not None:
        out["nearest_zone_side"] = "support"
    elif dr is not None:
        out["nearest_zone_side"] = "resistance"

    if in_resistance:
        r_pos = out.get("zone_position_in_resistance") or 0.0
        penalty = 30.0 + r_pos * 48.0
        out["zone_proximity_long_score"] = round(max(0.0, (out["zone_proximity_long_score"] or 0.0) - penalty), 2)
        out["zone_proximity_short_score"] = round(min(100.0, (out["zone_proximity_short_score"] or 0.0) + 8.0 + r_pos * 12.0), 2)
    if in_support:
        s_pos = out.get("zone_position_in_support") or 0.0
        penalty = 30.0 + (1.0 - s_pos) * 48.0
        out["zone_proximity_short_score"] = round(max(0.0, (out["zone_proximity_short_score"] or 0.0) - penalty), 2)
        out["zone_proximity_long_score"] = round(min(100.0, (out["zone_proximity_long_score"] or 0.0) + 8.0 + (1.0 - s_pos) * 12.0), 2)

    # Near-opposite-zone drag: even without being inside the zone, proximity to a strong opposing zone should fade the wrong-side score.
    if dr is not None and not in_resistance:
        if dr <= 0.03:
            out["zone_proximity_long_score"] = max(0.0, (out["zone_proximity_long_score"] or 0.0) - 34.0)
        elif dr <= 0.08:
            out["zone_proximity_long_score"] = max(0.0, (out["zone_proximity_long_score"] or 0.0) - 20.0)
    if ds is not None and not in_support:
        if ds <= 0.03:
            out["zone_proximity_short_score"] = max(0.0, (out["zone_proximity_short_score"] or 0.0) - 34.0)
        elif ds <= 0.08:
            out["zone_proximity_short_score"] = max(0.0, (out["zone_proximity_short_score"] or 0.0) - 20.0)

    out["zone_proximity_long_score"] = round(min(max(out["zone_proximity_long_score"], 0.0), 100.0), 2)
    out["zone_proximity_short_score"] = round(min(max(out["zone_proximity_short_score"], 0.0), 100.0), 2)
    return out


def zone_cluster_scores(d):
    def cluster(center, side):
        if center in (None, 0):
            return 0.0
        refs = []
        if side == "support":
            refs = [
                d.get("liq_below_1"), d.get("liq_below_2"), d.get("bear_trigger_price"),
                d.get("largest_bid_wall_price"), d.get("vwap_1h"), d.get("vwap_24h"),
                d.get("price_low_2_5m"), d.get("price_low_6_5m"), d.get("swing_low_12x5m"),
            ]
        else:
            refs = [
                d.get("liq_above_1"), d.get("liq_above_2"), d.get("bull_trigger_price"),
                d.get("largest_ask_wall_price"), d.get("vwap_1h"), d.get("vwap_24h"),
                d.get("price_high_2_5m"), d.get("price_high_6_5m"), d.get("swing_high_12x5m"),
            ]
        weights = [16, 10, 16, 12, 8, 6, 8, 8, 10]
        score = 0.0
        matches = 0
        for ref, w in zip(refs, weights):
            if ref in (None, 0):
                continue
            dist = abs(center - ref) / center * 100.0
            if dist <= 0.05:
                score += w; matches += 1
            elif dist <= 0.10:
                score += w * 0.75; matches += 1
            elif dist <= 0.18:
                score += w * 0.45
        score += min(matches * 4.0, 16.0)
        return round(min(score, 100.0), 2)

    support = cluster(d.get("support_zone_center"), "support")
    resistance = cluster(d.get("resistance_zone_center"), "resistance")
    active = 0.0
    if d.get("active_tested_side") == "support":
        active = support
    elif d.get("active_tested_side") == "resistance":
        active = resistance
    return {
        "support_cluster_score": support,
        "resistance_cluster_score": resistance,
        "active_zone_cluster_score": round(active, 2),
    }


def zone_fragility_features(d):
    s_touch = float(d.get("support_touch_count") or 0)
    s_rej = float(d.get("support_reject_count") or 0)
    s_bfail = float(d.get("support_break_fail_count") or 0)
    s_bq = float(d.get("support_bounce_quality_avg") or 0)
    r_touch = float(d.get("resistance_touch_count") or 0)
    r_rej = float(d.get("resistance_reject_count") or 0)
    r_bfail = float(d.get("resistance_break_fail_count") or 0)
    r_bq = float(d.get("resistance_bounce_quality_avg") or 0)

    support_exhaustion = min(max((s_touch - max(s_rej + s_bfail, 1)) * 5.5, 0.0), 100.0)
    resistance_exhaustion = min(max((r_touch - max(r_rej + r_bfail, 1)) * 5.5, 0.0), 100.0)

    support_fragility = min(max(support_exhaustion * 0.65 + max(0.0, 45.0 - s_bq) * 0.8, 0.0), 100.0)
    resistance_fragility = min(max(resistance_exhaustion * 0.65 + max(0.0, 45.0 - r_bq) * 0.8, 0.0), 100.0)
    active_fragility = 0.0
    if d.get("active_tested_side") == "support":
        active_fragility = support_fragility
    elif d.get("active_tested_side") == "resistance":
        active_fragility = resistance_fragility

    return {
        "support_exhaustion_score": round(support_exhaustion, 2),
        "resistance_exhaustion_score": round(resistance_exhaustion, 2),
        "support_fragility_score": round(support_fragility, 2),
        "resistance_fragility_score": round(resistance_fragility, 2),
        "zone_fragility_score": round(active_fragility, 2),
    }


def failed_break_detector(d, history_rows):
    rows = [row.get("btc", {}) for row in history_rows if isinstance(row, dict)][-6:]
    r_low = d.get("resistance_zone_low")
    r_high = d.get("resistance_zone_high")
    s_low = d.get("support_zone_low")
    s_high = d.get("support_zone_high")
    last = d.get("last")

    failed_long = False
    failed_short = False
    failed_long_strength = 0.0
    failed_short_strength = 0.0

    for row in rows:
        rh = r_high if r_high is not None else d.get("resistance_zone_center")
        sl = s_low if s_low is not None else d.get("support_zone_center")
        if rh not in (None, 0):
            high = row.get("price_high_2_5m") or row.get("prev_5m_high") or row.get("last")
            close = row.get("last")
            body = row.get("cur5m_body_pct_of_range") or 0.0
            vol = row.get("volume_quality_score") or 0.0
            if high is not None and close is not None and high > rh and close <= rh:
                failed_long = True
                strength = min(((high - rh) / rh) * 12000.0, 26.0) + min(body * 0.18, 18.0) + min(vol * 0.20, 20.0)
                failed_long_strength = max(failed_long_strength, strength)
        if sl not in (None, 0):
            low = row.get("price_low_2_5m") or row.get("prev_5m_low") or row.get("last")
            close = row.get("last")
            body = row.get("cur5m_body_pct_of_range") or 0.0
            vol = row.get("volume_quality_score") or 0.0
            if low is not None and close is not None and low < sl and close >= sl:
                failed_short = True
                strength = min(((sl - low) / sl) * 12000.0, 26.0) + min(body * 0.18, 18.0) + min(vol * 0.20, 20.0)
                failed_short_strength = max(failed_short_strength, strength)

    if last is not None and r_low is not None and r_high is not None and r_low <= last <= r_high:
        failed_long_strength += 6.0
    if last is not None and s_low is not None and s_high is not None and s_low <= last <= s_high:
        failed_short_strength += 6.0

    failed_long_strength = round(min(failed_long_strength, 100.0), 2)
    failed_short_strength = round(min(failed_short_strength, 100.0), 2)
    return {
        "failed_break_long": failed_long,
        "failed_break_short": failed_short,
        "failed_break_long_strength": failed_long_strength,
        "failed_break_short_strength": failed_short_strength,
        "break_accept_reject_score": max(failed_long_strength, failed_short_strength),
    }

def market_vs_trade_read(d):
    market_read = d.get("market_phase", "neutral")
    if d.get("direction_consensus_side") == "long":
        market_read = "bullish_bias"
    elif d.get("direction_consensus_side") == "short":
        market_read = "bearish_bias"

    trade_read = "not_tradeable"
    if d.get("hard_gate_pass") and (d.get("soft_score_total") or 0) >= 55:
        trade_read = "tradeable"
    elif (d.get("soft_score_total") or 0) >= 40:
        trade_read = "watch_only"

    why_not_higher = []
    if not d.get("hard_gate_pass"):
        why_not_higher.extend(d.get("hard_gate_blocks") or [])
    if (d.get("volume_spike_15m_x") or 0) < 0.8:
        why_not_higher.append("weak_15m_volume")
    if not d.get("bull_break_valid_v2") and d.get("direction_consensus_side") == "long":
        why_not_higher.append("no_long_break_validation")
    if not d.get("bear_break_valid_v2") and d.get("direction_consensus_side") == "short":
        why_not_higher.append("no_short_break_validation")
    if (d.get("zone_fragility_score") or 0) >= 70:
        why_not_higher.append("active_zone_fragile")

    return {
        "market_read": market_read,
        "trade_read": trade_read,
        "why_not_higher_tier": why_not_higher[:5],
    }


def level_memory_features(d, history_rows):
    support_zone = d.get("liq_below_1") or d.get("invalidation_long") or d.get("bear_trigger_price")
    resistance_zone = d.get("liq_above_1") or d.get("invalidation_short") or d.get("bull_trigger_price")

    support = level_zone_memory(history_rows, support_zone, side="support", band_pct=0.08, max_rows=30)
    resistance = level_zone_memory(history_rows, resistance_zone, side="resistance", band_pct=0.08, max_rows=30)

    support_band = support["zone_band_pct_used"] or 0.0
    resistance_band = resistance["zone_band_pct_used"] or 0.0
    support_low = support_zone * (1 - support_band / 100.0) if support_zone not in (None, 0) else None
    support_high = support_zone * (1 + support_band / 100.0) if support_zone not in (None, 0) else None
    resistance_low = resistance_zone * (1 - resistance_band / 100.0) if resistance_zone not in (None, 0) else None
    resistance_high = resistance_zone * (1 + resistance_band / 100.0) if resistance_zone not in (None, 0) else None

    out = {
        "support_zone_center": support_zone,
        "support_zone_low": round(support_low, 2) if support_low is not None else None,
        "support_zone_high": round(support_high, 2) if support_high is not None else None,
        "support_zone_width_pct": round(support_band * 2.0, 4),
        "support_touch_count": support["zone_touch_count"],
        "support_reject_count": support["zone_reject_count"],
        "support_reclaim_count": support["zone_reclaim_count"],
        "support_break_fail_count": support["zone_break_fail_count"],
        "support_hold_count": support["zone_hold_count"],
        "support_memory_score": support["zone_memory_score"],
        "support_bounce_count": support["zone_reject_count"] + support["zone_break_fail_count"],
        "support_bounce_quality_avg": support["zone_bounce_quality_avg"],
        "support_bounce_quality_peak": support["zone_bounce_quality_peak"],
        "support_band_pct_used": support["zone_band_pct_used"],
        "support_time_weighted_score": support["zone_time_weighted_score"],

        "resistance_zone_center": resistance_zone,
        "resistance_zone_low": round(resistance_low, 2) if resistance_low is not None else None,
        "resistance_zone_high": round(resistance_high, 2) if resistance_high is not None else None,
        "resistance_zone_width_pct": round(resistance_band * 2.0, 4),
        "resistance_touch_count": resistance["zone_touch_count"],
        "resistance_reject_count": resistance["zone_reject_count"],
        "resistance_reclaim_count": resistance["zone_reclaim_count"],
        "resistance_break_fail_count": resistance["zone_break_fail_count"],
        "resistance_hold_count": resistance["zone_hold_count"],
        "resistance_memory_score": resistance["zone_memory_score"],
        "resistance_reject_total": resistance["zone_reject_count"] + resistance["zone_break_fail_count"],
        "resistance_bounce_quality_avg": resistance["zone_bounce_quality_avg"],
        "resistance_bounce_quality_peak": resistance["zone_bounce_quality_peak"],
        "resistance_band_pct_used": resistance["zone_band_pct_used"],
        "resistance_time_weighted_score": resistance["zone_time_weighted_score"],
    }

    last = d.get("last")
    tested_side = "none"
    if last not in (None, 0):
        if support_zone and abs(last - support_zone) / last * 100 <= max(0.18, out["support_band_pct_used"] * 1.25):
            tested_side = "support"
        elif resistance_zone and abs(last - resistance_zone) / last * 100 <= max(0.18, out["resistance_band_pct_used"] * 1.25):
            tested_side = "resistance"

    out["active_tested_side"] = tested_side
    out["active_level_test_count"] = (
        out["support_touch_count"] if tested_side == "support"
        else out["resistance_touch_count"] if tested_side == "resistance"
        else 0
    )
    out["active_level_memory_score"] = (
        out["support_memory_score"] if tested_side == "support"
        else out["resistance_memory_score"] if tested_side == "resistance"
        else 0.0
    )
    out["active_level_bounce_quality"] = (
        out["support_bounce_quality_avg"] if tested_side == "support"
        else out["resistance_bounce_quality_avg"] if tested_side == "resistance"
        else 0.0
    )
    out["active_zone_low"] = (
        out["support_zone_low"] if tested_side == "support"
        else out["resistance_zone_low"] if tested_side == "resistance"
        else None
    )
    out["active_zone_high"] = (
        out["support_zone_high"] if tested_side == "support"
        else out["resistance_zone_high"] if tested_side == "resistance"
        else None
    )
    out["active_zone_width_pct"] = (
        out["support_zone_width_pct"] if tested_side == "support"
        else out["resistance_zone_width_pct"] if tested_side == "resistance"
        else 0.0
    )
    return out



def acceptance_engine(d, history_rows):
    rows = [row.get("btc", {}) for row in history_rows if isinstance(row, dict)][-8:]
    last = d.get("last")
    bull = d.get("bull_trigger_price")
    bear = d.get("bear_trigger_price")

    long_holds = 0
    short_holds = 0
    long_body_support = 0.0
    short_body_support = 0.0
    long_volume_support = 0.0
    short_volume_support = 0.0

    for row in rows:
        r_last = row.get("last")
        body = row.get("cur5m_body_pct_of_range") or 0.0
        volq = row.get("volume_quality_score") or 0.0
        if bull not in (None, 0) and r_last is not None and r_last >= bull:
            long_holds += 1
            long_body_support += min(body * 0.25, 20.0)
            long_volume_support += min(volq * 0.20, 20.0)
        if bear not in (None, 0) and r_last is not None and r_last <= bear:
            short_holds += 1
            short_body_support += min(body * 0.25, 20.0)
            short_volume_support += min(volq * 0.20, 20.0)

    long_retest_success = bool(d.get("retest_long_ready")) and (d.get("retest_winner_side") == "long")
    short_retest_success = bool(d.get("retest_short_ready")) and (d.get("retest_winner_side") == "short")

    long_acceptance = 0.0
    short_acceptance = 0.0

    if bull not in (None, 0) and last is not None:
        dist = (last - bull) / bull * 100.0
        if dist >= 0:
            long_acceptance += min(dist * 120, 18.0)
        long_acceptance += min(long_holds * 9.0, 27.0)
        long_acceptance += min(long_body_support / max(long_holds, 1), 18.0) if long_holds else 0.0
        long_acceptance += min(long_volume_support / max(long_holds, 1), 18.0) if long_holds else 0.0
        if long_retest_success:
            long_acceptance += 12.0
        if d.get("dominant_bias_htf") == "long":
            long_acceptance += 8.0
        elif d.get("dominant_bias_htf") == "short":
            long_acceptance -= 8.0

    if bear not in (None, 0) and last is not None:
        dist = (bear - last) / bear * 100.0
        if dist >= 0:
            short_acceptance += min(dist * 120, 18.0)
        short_acceptance += min(short_holds * 9.0, 27.0)
        short_acceptance += min(short_body_support / max(short_holds, 1), 18.0) if short_holds else 0.0
        short_acceptance += min(short_volume_support / max(short_holds, 1), 18.0) if short_holds else 0.0
        if short_retest_success:
            short_acceptance += 12.0
        if d.get("dominant_bias_htf") == "short":
            short_acceptance += 8.0
        elif d.get("dominant_bias_htf") == "long":
            short_acceptance -= 8.0

    return {
        "acceptance_hold_bars_5m_long": long_holds,
        "acceptance_hold_bars_5m_short": short_holds,
        "acceptance_retest_success_long": long_retest_success,
        "acceptance_retest_success_short": short_retest_success,
        "acceptance_volume_support_long": round(min(long_volume_support, 100.0), 2),
        "acceptance_volume_support_short": round(min(short_volume_support, 100.0), 2),
        "long_acceptance_score": round(min(max(long_acceptance, 0.0), 100.0), 2),
        "short_acceptance_score": round(min(max(short_acceptance, 0.0), 100.0), 2),
        "acceptance_quality_score": round(max(long_acceptance, short_acceptance), 2),
    }

def breakout_validation_v2(d):
    bull = 0.0
    bear = 0.0
    dominant = d.get("dominant_bias_htf", "neutral")
    ctp_long = d.get("countertrend_penalty_long") or 0.0
    ctp_short = d.get("countertrend_penalty_short") or 0.0

    if (d.get("last") or 0) >= (d.get("bull_trigger_price") or 10**18):
        bull += 18
    bull += min((d.get("breakout_quality_score") or 0) * 0.45, 20.0)
    bull += min((d.get("breakout_quality_v2") or 0) * 0.20, 15.0)
    bull += min((d.get("long_acceptance_score") or 0) * 0.35, 20.0)
    bull += min((d.get("volume_quality_score") or 0) * 0.12, 10.0)
    bull += min((d.get("breakout_confirmation_cluster") or 0) * 0.18, 12.0)
    if dominant == "long":
        bull += 8
    elif dominant == "short":
        bull -= ctp_long * 0.5
    if (d.get("cur5m_upper_wick_pct_of_range") or 0) > 55:
        bull -= 10
    if (d.get("signal_conflict_score") or 0) >= 45:
        bull -= 8

    if (d.get("last") or 10**18) <= (d.get("bear_trigger_price") or -10**18):
        bear += 18
    bear += min((d.get("breakout_quality_score") or 0) * 0.45, 20.0)
    bear += min((d.get("breakout_quality_v2") or 0) * 0.20, 15.0)
    bear += min((d.get("short_acceptance_score") or 0) * 0.35, 20.0)
    bear += min((d.get("volume_quality_score") or 0) * 0.12, 10.0)
    bear += min((d.get("breakout_confirmation_cluster") or 0) * 0.18, 12.0)
    if dominant == "short":
        bear += 8
    elif dominant == "long":
        bear -= ctp_short * 0.5
    if (d.get("cur5m_lower_wick_pct_of_range") or 0) > 55:
        bear -= 10
    if (d.get("signal_conflict_score") or 0) >= 45:
        bear -= 8

    bull = max(0.0, min(100.0, bull))
    bear = max(0.0, min(100.0, bear))

    bull_threshold = 58 if dominant != "short" else 64
    bear_threshold = 58 if dominant != "long" else 64

    return {
        "bull_break_valid_v2": bull >= bull_threshold,
        "bear_break_valid_v2": bear >= bear_threshold,
        "breakout_acceptance_window_score": round(max(bull, bear), 2),
        "breakout_fail_risk": round(
            max(
                0.0,
                min(
                    100.0,
                    35.0
                    + max(d.get("signal_conflict_score") or 0, 0) * 0.4
                    + (10.0 if (d.get("cur5m_upper_wick_pct_of_range") or 0) > 60 else 0.0)
                    + (10.0 if (d.get("cur5m_lower_wick_pct_of_range") or 0) > 60 else 0.0)
                    - max(d.get("acceptance_quality_score") or 0, 0) * 0.25,
                )
            ),
            2,
        ),
    }

