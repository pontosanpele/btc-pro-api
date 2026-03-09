from btc_pro_config import clamp, SOFT_SCORE_WEIGHTS_V2
from btc_pro_metrics import regime_adaptive_weights

def compute_scores(d):
    trend_align=clamp(((d.get('multi_tf_alignment_score') or 0)+3)/6*100,0,100); breakout=d.get('breakout_quality_score') or 0.0; orderflow=abs(d.get('delta_strength_score') or 0.0); volume_q=d.get('volume_quality_score') or 0.0
    confidence=round((trend_align*0.35)+(breakout*0.30)+(orderflow*0.20)+(volume_q*0.15),2)
    bull=bear=0.0; funding=d.get('funding_pct') or 0.0; delta=d.get('recent_notional_delta_pct') or 0.0; p1=d.get('price_vs_vwap_1h_pct') or 0.0; ob=d.get('orderbook_imbalance_0_25_pct') or 0.0
    if funding<0: bull += min(abs(funding)*1200,18)
    if funding>0: bear += min(abs(funding)*1200,18)
    if delta>0: bull += min(delta*0.9,20)
    if delta<0: bear += min(abs(delta)*0.9,20)
    if p1>0: bull += min(p1*18,10)
    if p1<0: bear += min(abs(p1)*18,10)
    if ob>0: bull += min(ob*0.35,10)
    if ob<0: bear += min(abs(ob)*0.35,10)
    if d.get('absorption_buying'): bull += 7
    if d.get('absorption_selling'): bear += 7
    if d.get('bull_divergence'): bull += 10
    if d.get('bear_divergence'): bear += 10
    align=d.get('multi_tf_alignment_score') or 0
    if align>0: bull += align*4
    if align<0: bear += abs(align)*4
    if d.get('bull_break_valid'): bull += 10
    if d.get('bear_break_valid'): bear += 10
    bull=round(bull,2); bear=round(bear,2); raw='long' if bull>=bear+8 else 'short' if bear>=bull+8 else 'no_trade'
    return {'bull_score':bull,'bear_score':bear,'confidence_score':confidence,'raw_trade_bias':raw,'trade_bias':raw}

def signal_agreement(d):
    long_agree=short_agree=0
    if d.get('trade_bias')=='long': long_agree += 1
    if d.get('trade_bias')=='short': short_agree += 1
    align=d.get('multi_tf_alignment_score') or 0
    if align>0: long_agree += 1
    if align<0: short_agree += 1
    if d.get('absorption_buying'): long_agree += 1
    if d.get('absorption_selling'): short_agree += 1
    if d.get('bull_divergence'): long_agree += 1
    if d.get('bear_divergence'): short_agree += 1
    if d.get('bull_break_valid'): long_agree += 1
    if d.get('bear_break_valid'): short_agree += 1
    if d.get('nearest_liquidity_side')=='above': long_agree += 1
    if d.get('nearest_liquidity_side')=='below': short_agree += 1
    return {'signal_agreement_long':long_agree,'signal_agreement_short':short_agree}

def setup_readiness(d):
    conf=d.get('confidence_score') or 0.0; bo=d.get('breakout_quality_score') or 0.0; agreement=max((d.get('signal_agreement_long') or 0),(d.get('signal_agreement_short') or 0))*20; volq=d.get('volume_quality_score') or 0.0; penalty=(d.get('no_trade_score') or 0.0)*0.35 + max((d.get('bull_trap_risk') or 0),(d.get('bear_trap_risk') or 0))*0.25
    score=(conf*0.30)+(bo*0.25)+(agreement*0.20)+(volq*0.15)-penalty*0.10
    return {'setup_readiness_score':round(clamp(score,0,100),2)}

def alert_priority(d):
    primary='none'
    if d.get('bull_break_valid'): primary='bull_break_valid'
    elif d.get('bear_break_valid'): primary='bear_break_valid'
    elif d.get('retest_long_ready'): primary='retest_long_ready'
    elif d.get('retest_short_ready'): primary='retest_short_ready'
    elif d.get('early_setup_alert') not in (None,'none'): primary=d.get('early_setup_alert')
    elif d.get('trade_bias')!='no_trade': primary=f"bias_{d.get('trade_bias')}"
    score=(d.get('setup_readiness_score') or 0)+(15 if primary!='none' else 0)
    return {'primary_alert':primary,'alert_priority_score':round(clamp(score,0,100),2)}

def bias_confirmation(d):
    raw=d.get('raw_trade_bias','no_trade'); prev=d.get('prev_trade_bias'); conf=d.get('confidence_score') or 0.0; bull_valid=d.get('bull_break_valid'); bear_valid=d.get('bear_break_valid'); primary=d.get('primary_alert','none'); setup_type=d.get('setup_type',''); stage=d.get('trade_stage',''); long_hint=0; short_hint=0
    if primary in ('bull_break_valid','retest_long_ready','possible_long_setup_near','bias_long'): long_hint += 2
    if primary in ('bear_break_valid','retest_short_ready','possible_short_setup_near','bias_short'): short_hint += 2
    if setup_type in ('long_retest_setup','range_reclaim_long','short_squeeze_attempt','flush_bounce_setup'): long_hint += 1
    if setup_type in ('short_retest_setup','range_reject_short','liquidation_flush','failed_breakout_setup'): short_hint += 1
    if stage in ('breakout_acceptance','retest_long_ready'): long_hint += 1
    if stage in ('breakdown_acceptance','retest_short_ready'): short_hint += 1
    if d.get('above_long_trigger_acceptance'): long_hint += 1
    if d.get('below_short_trigger_acceptance'): short_hint += 1
    long_hint += 1 if (d.get('signal_agreement_long') or 0)>=3 else 0
    short_hint += 1 if (d.get('signal_agreement_short') or 0)>=3 else 0
    readiness=d.get('setup_readiness_score') or 0.0
    if readiness>=45:
        if short_hint>=long_hint+2: raw='short'
        elif long_hint>=short_hint+2: raw='long'
    confirmed=raw; pending=False; confirmed_flip=False; arbitration_side='none'; arbitration_strength=abs(long_hint-short_hint)
    if raw=='long' and short_hint>=long_hint+2 and not bull_valid:
        if readiness>=50 and (d.get('retest_short_ready') or d.get('bear_break_valid')): confirmed='short'; arbitration_side='short_override'; confirmed_flip=True
        else: confirmed='no_trade'; arbitration_side='neutralized_to_avoid_long_mismatch'; pending=True
    elif raw=='short' and long_hint>=short_hint+2 and not bear_valid:
        if readiness>=50 and (d.get('retest_long_ready') or d.get('bull_break_valid')): confirmed='long'; arbitration_side='long_override'; confirmed_flip=True
        else: confirmed='no_trade'; arbitration_side='neutralized_to_avoid_short_mismatch'; pending=True
    if confirmed!='no_trade' and prev not in (None,confirmed):
        valid=(confirmed=='long' and (bull_valid or d.get('retest_long_ready'))) or (confirmed=='short' and (bear_valid or d.get('retest_short_ready')))
        if conf<45 and readiness<45 and not valid: confirmed='no_trade'; pending=True; confirmed_flip=False
        else: confirmed_flip=True
    return {'trade_bias':confirmed,'bias_flip_pending':pending,'bias_flip_confirmed':confirmed_flip,'bias_arbitration_side':arbitration_side,'bias_arbitration_strength':arbitration_strength,'long_hint_score':long_hint,'short_hint_score':short_hint}

def direction_consensus(d):
    long_score=short_score=0.0
    if d.get('raw_trade_bias')=='long': long_score += 20
    elif d.get('raw_trade_bias')=='short': short_score += 20
    if d.get('trade_bias')=='long': long_score += 25
    elif d.get('trade_bias')=='short': short_score += 25
    if d.get('primary_alert') in ('retest_long_ready','possible_long_setup_near','bull_break_valid','bias_long'): long_score += 20
    if d.get('primary_alert') in ('retest_short_ready','possible_short_setup_near','bear_break_valid','bias_short'): short_score += 20
    if d.get('setup_type') in ('long_retest_setup','range_reclaim_long','short_squeeze_attempt','flush_bounce_setup'): long_score += 15
    if d.get('setup_type') in ('short_retest_setup','range_reject_short','liquidation_flush','failed_breakout_setup'): short_score += 15
    if d.get('trade_stage') in ('breakout_acceptance','retest_long_ready','watch_long'): long_score += 10
    if d.get('trade_stage') in ('breakdown_acceptance','retest_short_ready','watch_short'): short_score += 10
    if d.get('above_long_trigger_acceptance'): long_score += 10
    if d.get('below_short_trigger_acceptance'): short_score += 10
    if d.get('retest_winner_side')=='long': long_score += 15
    elif d.get('retest_winner_side')=='short': short_score += 15
    if d.get('nearest_liquidity_side')=='above': long_score += 5
    elif d.get('nearest_liquidity_side')=='below': short_score += 5
    if d.get('orderflow_consistency_side')=='bullish': long_score += 10
    elif d.get('orderflow_consistency_side')=='bearish': short_score += 10
    long_score=round(min(max(long_score,0),100),2); short_score=round(min(max(short_score,0),100),2); side='neutral'
    if long_score>=short_score+12: side='long'
    elif short_score>=long_score+12: side='short'
    return {'direction_consensus_long_score':long_score,'direction_consensus_short_score':short_score,'direction_consensus_side':side,'direction_consensus_score':round(abs(long_score-short_score),2)}

def signal_conflict_detector(d):
    long_votes=short_votes=0
    checks=[('raw_trade_bias',('long',),('short',)),('trade_bias',('long',),('short',)),('primary_alert',('retest_long_ready','possible_long_setup_near','bull_break_valid','bias_long'),('retest_short_ready','possible_short_setup_near','bear_break_valid','bias_short')),('setup_type',('long_retest_setup','range_reclaim_long','short_squeeze_attempt','flush_bounce_setup'),('short_retest_setup','range_reject_short','liquidation_flush','failed_breakout_setup'))]
    for key,long_vals,short_vals in checks:
        v=d.get(key)
        if v in long_vals: long_votes += 1
        if v in short_vals: short_votes += 1
    if d.get('retest_winner_side')=='long': long_votes += 1
    elif d.get('retest_winner_side')=='short': short_votes += 1
    if d.get('direction_consensus_side')=='long': long_votes += 1
    elif d.get('direction_consensus_side')=='short': short_votes += 1
    if d.get('orderflow_consistency_side')=='bullish': long_votes += 1
    elif d.get('orderflow_consistency_side')=='bearish': short_votes += 1
    diff=abs(long_votes-short_votes); conflict=0 if diff>=3 else 25 if diff==2 else 55 if diff==1 else 75; reason='aligned'
    if conflict>=55: reason='high_internal_direction_conflict'
    elif conflict>=25: reason='moderate_internal_direction_conflict'
    return {'signal_conflict_score':round(conflict,2),'signal_conflict_reason':reason}


def compute_scores_v2(d):
    w = regime_adaptive_weights(d.get("market_regime"))
    flow = d.get("flow_alignment_score") or 0.0
    breakout = d.get("breakout_quality_v2") or d.get("breakout_quality_score") or 0.0
    retest = d.get("retest_quality_score") or 0.0
    volume = d.get("volume_quality_score") or 0.0
    execution = d.get("execution_feasibility_score") or 0.0
    context = max(0.0, 100 - (d.get("context_penalty_score") or 0.0))

    confidence_v2 = round(
        flow * w["flow"]
        + volume * w["volume"]
        + breakout * w["structure"] * 0.75
        + execution * w["execution"]
        + context * w["context"] * 0.85,
        2,
    )

    bull = bear = 0.0
    if (d.get("direction_consensus_side") == "long"):
        bull += 20
    elif (d.get("direction_consensus_side") == "short"):
        bear += 20

    if (d.get("orderflow_consistency_side") == "bullish"):
        bull += 12
    elif (d.get("orderflow_consistency_side") == "bearish"):
        bear += 12

    if (d.get("retest_winner_side") == "long"):
        bull += min(retest * 0.22, 18)
    elif (d.get("retest_winner_side") == "short"):
        bear += min(retest * 0.22, 18)

    if (d.get("breakout_direction") == "up"):
        bull += min(breakout * 0.18, 20)
    elif (d.get("breakout_direction") == "down"):
        bear += min(breakout * 0.18, 20)

    if (d.get("nearest_liquidity_side") == "above"):
        bull += 6
    elif (d.get("nearest_liquidity_side") == "below"):
        bear += 6

    bull += min((d.get("bull_score_slope_3") or 0.0) * 4.0, 10) if (d.get("bull_score_slope_3") or 0.0) > 0 else 0
    bear += min(abs(d.get("bear_score_slope_3") or 0.0) * 4.0, 10) if (d.get("bear_score_slope_3") or 0.0) > 0 else 0

    bull = round(clamp(bull, 0, 100), 2)
    bear = round(clamp(bear, 0, 100), 2)

    raw = "long" if bull >= bear + 8 else "short" if bear >= bull + 8 else "no_trade"
    return {
        "bull_score_v2": bull,
        "bear_score_v2": bear,
        "confidence_score_v2": confidence_v2,
        "raw_trade_bias_v2": raw,
    }


def market_and_trading_bias(d):
    market_bias = d.get("direction_consensus_side", "neutral")
    trade_bias = d.get("trade_bias", "no_trade")

    # Trade bias should be stricter than market bias.
    if d.get("no_trade_active"):
        trade_bias = "no_trade"
    elif (d.get("signal_conflict_score") or 0) >= 55:
        trade_bias = "no_trade"
    elif (d.get("execution_feasibility_score") or 0) < 45:
        trade_bias = "no_trade"
    elif (d.get("expected_value_score") or 0) < 35:
        trade_bias = "no_trade"

    return {
        "market_bias": market_bias,
        "trading_bias": trade_bias,
    }

def hard_gate_evaluation(d):
    hard_blocks = []

    no_trade_active = bool(d.get("no_trade_active"))
    override_active = bool(d.get("no_trade_override_active"))
    if no_trade_active and not override_active:
        hard_blocks.append("no_trade_filter_active")
    if (d.get("signal_conflict_score") or 0) >= 55:
        hard_blocks.append("signal_conflict_high")
    if (d.get("execution_feasibility_score") or 0) < 45:
        hard_blocks.append("execution_feasibility_too_low")
    if (d.get("invalidation_quality_score") or 0) < 35:
        hard_blocks.append("invalidation_quality_too_low")
    if (d.get("context_penalty_score") or 0) >= 40:
        hard_blocks.append("context_penalty_too_high")

    hard_pass = len(hard_blocks) == 0
    return {
        "hard_gate_pass": hard_pass,
        "hard_gate_blocks": hard_blocks,
    }

def soft_score_stack(d):
    trigger_behavior = max(
        d.get("trigger_behavior_long_score") or 0.0,
        d.get("trigger_behavior_short_score") or 0.0,
    )
    components = {
        "expected_value": d.get("expected_value_score") or 0.0,
        "entry_timing": d.get("entry_timing_score") or 0.0,
        "execution": d.get("execution_feasibility_score") or 0.0,
        "setup_readiness_v2": d.get("setup_readiness_v2") or d.get("setup_readiness_score") or 0.0,
        "retest_quality": d.get("retest_quality_score") or 0.0,
        "breakout_quality_v2": d.get("breakout_quality_v2") or d.get("breakout_quality_score") or 0.0,
        "trigger_behavior": trigger_behavior,
    }
    weighted = sum(components[k] * SOFT_SCORE_WEIGHTS_V2[k] for k in SOFT_SCORE_WEIGHTS_V2)
    return {
        "soft_score_total": round(weighted, 2),
        "soft_score_components": components,
    }

