from btc_pro_config import clamp

def detect_regime(d):
    funding=d.get('funding_pct'); oi5=d.get('oi_change_5m_pct'); vol5=d.get('volume_spike_5m_x'); vol15=d.get('volume_spike_15m_x'); delta=d.get('recent_notional_delta_pct'); p1=d.get('price_vs_vwap_1h_pct'); chop=d.get('chop_score')
    if funding is not None and delta is not None and oi5 is not None:
        if funding<0 and delta>12 and oi5<0: return 'short_squeeze'
        if delta<-12 and oi5<0: return 'long_flush'
        if delta>10 and oi5>0 and vol5 is not None and vol5>=1.2: return 'trend_build_long'
        if delta<-10 and oi5>0 and vol5 is not None and vol5>=1.2: return 'trend_build_short'
    if chop is not None and chop<0.7 and (vol5 or 0)<0.8 and (vol15 or 0)<0.8: return 'low_liquidity_range'
    if p1 is not None and d.get('range_expansion_5m_x') is not None:
        if p1>0 and d['range_expansion_5m_x']>1.3: return 'impulse_up'
        if p1<0 and d['range_expansion_5m_x']>1.3: return 'impulse_down'
    return 'neutral'

def reversal_probability(d):
    long_p=short_p=0.0
    if d.get('bull_divergence'): long_p += 28
    if d.get('bear_divergence'): short_p += 28
    if d.get('absorption_buying'): long_p += 18
    if d.get('absorption_selling'): short_p += 18
    rp=d.get('range_pos_pct')
    if rp is not None:
        if rp<20: long_p += 10
        elif rp>80: short_p += 10
    long_p += (d.get('move_exhaustion_down') or 0)*0.35
    short_p += (d.get('move_exhaustion_up') or 0)*0.35
    return {'reversal_probability_long':round(clamp(long_p,0,100),2),'reversal_probability_short':round(clamp(short_p,0,100),2)}

def trade_lifecycle_state(d):
    stage='range'; phase='neutral'
    if d.get('bull_break_valid'): stage='breakout_acceptance'; phase='bullish'
    elif d.get('bear_break_valid'): stage='breakdown_acceptance'; phase='bearish'
    elif d.get('above_long_trigger_acceptance'): stage='breakout_attempt'; phase='bullish'
    elif d.get('below_short_trigger_acceptance'): stage='breakdown_attempt'; phase='bearish'
    if d.get('retest_long_ready'): stage='retest_long_ready'; phase='bullish'
    if d.get('retest_short_ready'): stage='retest_short_ready'; phase='bearish'
    if (d.get('reversal_probability_long') or 0)>=60: phase='possible_bull_reversal'
    if (d.get('reversal_probability_short') or 0)>=60: phase='possible_bear_reversal'
    return {'trade_stage':stage,'market_phase':phase}

def setup_classifier(d):
    regime=d.get('market_regime'); bias=d.get('trade_bias'); breakout_dir=d.get('breakout_direction'); vol15=d.get('volume_spike_15m_x') or 0
    if regime=='short_squeeze' or (bias=='long' and breakout_dir=='up' and vol15>1.0): return {'setup_type':'short_squeeze_attempt'}
    if regime=='long_flush' or (bias=='short' and breakout_dir=='down' and vol15>1.0): return {'setup_type':'liquidation_flush'}
    if d.get('retest_long_ready'): return {'setup_type':'long_retest_setup'}
    if d.get('retest_short_ready'): return {'setup_type':'short_retest_setup'}
    if d.get('absorption_buying') or d.get('bull_divergence'): return {'setup_type':'flush_bounce_setup'}
    if d.get('absorption_selling') or d.get('bear_divergence'): return {'setup_type':'failed_breakout_setup'}
    if bias=='long' and d.get('nearest_liquidity_side')=='above': return {'setup_type':'range_reclaim_long'}
    if bias=='short' and d.get('nearest_liquidity_side')=='below': return {'setup_type':'range_reject_short'}
    return {'setup_type':'no_trade_chop'}

def summary_generator(d):
    status='WAIT'; reason=[]; watch=None
    if d.get('trade_bias')=='long' and not d.get('no_trade_active'): status='LONG_READY' if d.get('bull_break_valid') or d.get('retest_long_ready') else 'LONG_WATCH'
    elif d.get('trade_bias')=='short' and not d.get('no_trade_active'): status='SHORT_READY' if d.get('bear_break_valid') or d.get('retest_short_ready') else 'SHORT_WATCH'
    elif d.get('primary_alert') not in (None,'none'): status='WATCH'
    if d.get('no_trade_active'): reason.extend(d.get('no_trade_reason') or [])
    if d.get('primary_alert') not in (None,'none'): reason.append(d.get('primary_alert'))
    if d.get('bias_arbitration_side') not in (None,'none'): reason.append(d.get('bias_arbitration_side'))
    if d.get('trade_bias')=='long': watch=d.get('bull_trigger_price')
    elif d.get('trade_bias')=='short': watch=d.get('bear_trigger_price')
    elif d.get('primary_alert') in ('retest_short_ready','possible_short_setup_near','bear_break_valid'): watch=d.get('bear_trigger_price')
    elif d.get('primary_alert') in ('retest_long_ready','possible_long_setup_near','bull_break_valid'): watch=d.get('bull_trigger_price')
    reason=[x for x in reason if x]
    return {'summary_status':status,'summary_reason':reason[:3],'summary_watch_level':watch}

def state_machine_v2(d):
    state='neutral_range'
    if d.get('bull_break_valid'): state='accepted_long'
    elif d.get('bear_break_valid'): state='accepted_short'
    elif d.get('retest_winner_side')=='long' and d.get('retest_long_ready'): state='retest_long'
    elif d.get('retest_winner_side')=='short' and d.get('retest_short_ready'): state='retest_short'
    elif d.get('primary_alert')=='possible_long_setup_near': state='watch_long'
    elif d.get('primary_alert')=='possible_short_setup_near': state='watch_short'
    elif d.get('above_long_trigger_acceptance'): state='breakout_attempt_long'
    elif d.get('below_short_trigger_acceptance'): state='breakout_attempt_short'
    return {'state_machine_v2':state}

def final_recommendation(d):
    action='WAIT'; side='none'; priority='low'; reason=[]
    if d.get('no_trade_active'):
        action='NO_TRADE_CHOP'; reason.extend(d.get('no_trade_reason') or [])
    elif d.get('bull_break_valid') and (d.get('entry_timing_score') or 0)>=60:
        action='LONG_TRIGGER_READY'; side='long'; priority='high'; reason.append('bull_break_valid')
    elif d.get('bear_break_valid') and (d.get('entry_timing_score') or 0)>=60:
        action='SHORT_TRIGGER_READY'; side='short'; priority='high'; reason.append('bear_break_valid')
    elif d.get('direction_consensus_side')=='long':
        action='WATCH_LONG'; side='long'; priority='medium'; reason.append('direction_consensus_long')
    elif d.get('direction_consensus_side')=='short':
        action='WATCH_SHORT'; side='short'; priority='medium'; reason.append('direction_consensus_short')
    if d.get('signal_conflict_score',0)>=55:
        action='WAIT'; priority='low'; reason.append('signal_conflict')
    return {'final_action':action,'final_side':side,'final_priority':priority,'final_reason':reason[:3]}

def state_change_alerts(d):
    alerts=[]
    if d.get('trade_bias')=='no_trade': alerts.append('bias_is_no_trade')
    if d.get('breakout_quality_score') is not None and d['breakout_quality_score']>=55: alerts.append('breakout_quality_high')
    if d.get('trap_alert') and d['trap_alert']!='none': alerts.append(d['trap_alert'])
    if d.get('no_trade_active'): alerts.append('no_trade_filter_active')
    if d.get('bull_divergence'): alerts.append('bull_divergence_detected')
    if d.get('bear_divergence'): alerts.append('bear_divergence_detected')
    if d.get('bias_changed'): alerts.append('bias_changed')
    if d.get('regime_changed'): alerts.append('regime_changed')
    if d.get('quality_jump'): alerts.append('quality_jump')
    if d.get('trap_risk_increased'): alerts.append('trap_risk_increased')
    if d.get('no_trade_activated'): alerts.append('no_trade_activated')
    if d.get('trade_plan_invalidated'): alerts.append('trade_plan_invalidated')
    if d.get('early_setup_alert') not in (None,'none'): alerts.append(d['early_setup_alert'])
    return {'alerts':alerts}


def hierarchical_decision_engine(d):
    market_state = d.get("market_regime") or "neutral"
    direction = d.get("direction_consensus_side") or "neutral"
    setup = d.get("setup_type") or "no_trade_chop"

    tradeability = "not_tradeable"
    if not d.get("no_trade_active") and (d.get("expected_value_score") or 0) >= 45 and (d.get("execution_feasibility_score") or 0) >= 50:
        tradeability = "tradeable"
    elif (d.get("setup_readiness_score") or 0) >= 35:
        tradeability = "borderline"

    timing = "early"
    if d.get("bull_break_valid") or d.get("bear_break_valid"):
        timing = "confirmed"
    elif d.get("retest_winner_side") != "none":
        timing = "retest_ready"
    elif (d.get("entry_timing_score") or 0) >= 60:
        timing = "actionable_watch"
    elif (d.get("entry_timing_score") or 0) >= 45:
        timing = "watch"
    elif (d.get("late_entry_risk") or 0) >= 35:
        timing = "late"

    return {
        "decision_market_state": market_state,
        "decision_direction": direction,
        "decision_setup_type": setup,
        "decision_tradeability": tradeability,
        "decision_timing": timing,
    }

def final_recommendation_v2(d):
    action = "WAIT"
    side = "none"
    priority = "low"
    reason = []

    direction = d.get("direction_consensus_side") or "neutral"
    tradeability = d.get("decision_tradeability") or "not_tradeable"
    timing = d.get("decision_timing") or "early"
    ev = d.get("expected_value_score") or 0.0
    conflict = d.get("signal_conflict_score") or 0.0

    if d.get("no_trade_active"):
        action = "NO_TRADE_CHOP"
        reason.extend(d.get("no_trade_reason") or [])
    elif conflict >= 55:
        action = "WAIT"
        reason.append("signal_conflict")
    elif direction == "long":
        side = "long"
        if d.get("bull_break_valid") and timing in ("confirmed", "actionable_watch") and ev >= 55:
            action = "LONG_TRIGGER_READY"
            priority = "high"
            reason.append("bull_break_valid")
        elif d.get("retest_winner_side") == "long" and tradeability in ("tradeable", "borderline"):
            action = "LONG_RETEST_READY"
            priority = "medium"
            reason.append("long_retest_winner")
        elif timing in ("watch", "actionable_watch", "early"):
            action = "WATCH_LONG"
            priority = "medium"
            reason.append("direction_consensus_long")
    elif direction == "short":
        side = "short"
        if d.get("bear_break_valid") and timing in ("confirmed", "actionable_watch") and ev >= 55:
            action = "SHORT_TRIGGER_READY"
            priority = "high"
            reason.append("bear_break_valid")
        elif d.get("retest_winner_side") == "short" and tradeability in ("tradeable", "borderline"):
            action = "SHORT_RETEST_READY"
            priority = "medium"
            reason.append("short_retest_winner")
        elif timing in ("watch", "actionable_watch", "early"):
            action = "WATCH_SHORT"
            priority = "medium"
            reason.append("direction_consensus_short")

    if d.get("execution_feasibility_score", 100) < 45 or ev < 35:
        if action not in ("NO_TRADE_CHOP", "WAIT"):
            action = "WAIT"
            priority = "low"
            reason.append("poor_tradeability")

    return {
        "final_action_v2": action,
        "final_side_v2": side,
        "final_priority_v2": priority,
        "final_reason_v2": reason[:4],
    }


def decision_hysteresis_v2(d):
    prev_bias = d.get("prev_trade_bias")
    prev_regime = d.get("prev_market_regime")
    stability = d.get("decision_stability_score") or 0.0
    ev = d.get("expected_value_score") or 0.0
    ctx = d.get("context_penalty_score") or 0.0
    conflict = d.get("signal_conflict_score") or 0.0

    hold = False
    hysteresis_score = 0.0

    if stability >= 70:
        hysteresis_score += 25
    if conflict < 25:
        hysteresis_score += 20
    if ctx < 20:
        hysteresis_score += 20
    if ev >= 45:
        hysteresis_score += 20
    if d.get("bias_persistence_count", 0) >= 1:
        hysteresis_score += 15

    if prev_bias == d.get("trade_bias") and prev_bias not in (None, "no_trade"):
        hold = True
    if prev_regime == d.get("market_regime") and stability >= 75:
        hold = True

    return {
        "decision_hysteresis_score": round(min(max(hysteresis_score, 0), 100), 2),
        "decision_hold_preference": hold,
    }

def final_judgment_tiers(d):
    direction = d.get("direction_consensus_side") or "neutral"
    ev = d.get("expected_value_score") or 0.0
    timing = d.get("entry_timing_score") or 0.0
    execq = d.get("execution_feasibility_score") or 0.0
    ctx = d.get("context_penalty_score") or 0.0
    invq = d.get("invalidation_quality_score") or 0.0
    conflict = d.get("signal_conflict_score") or 0.0

    tier = "WAIT"
    side = "none"
    reasons = []

    if d.get("no_trade_active") or conflict >= 55:
        return {
            "final_tier": "WAIT",
            "final_tier_side": "none",
            "final_tier_reasons": (d.get("no_trade_reason") or [])[:3] or ["signal_conflict"],
        }

    if direction == "long":
        side = "long"
        if d.get("bull_break_valid") and timing >= 65 and ev >= 58 and execq >= 58 and ctx < 25:
            tier = "LONG_TRIGGER_READY"
            reasons.append("bull_break_valid")
        elif d.get("retest_winner_side") == "long" and ev >= 48 and invq >= 55:
            tier = "LONG_RETEST_READY"
            reasons.append("long_retest_winner")
        elif timing >= 52 and ev >= 42:
            tier = "LONG_CONFIRM_WAIT"
            reasons.append("await_long_confirmation")
        elif ev >= 35:
            tier = "WATCH_LONG"
            reasons.append("direction_consensus_long")
        else:
            tier = "EARLY_LONG"
            reasons.append("early_long_bias")

    elif direction == "short":
        side = "short"
        if d.get("bear_break_valid") and timing >= 65 and ev >= 58 and execq >= 58 and ctx < 25:
            tier = "SHORT_TRIGGER_READY"
            reasons.append("bear_break_valid")
        elif d.get("retest_winner_side") == "short" and ev >= 48 and invq >= 55:
            tier = "SHORT_RETEST_READY"
            reasons.append("short_retest_winner")
        elif timing >= 52 and ev >= 42:
            tier = "SHORT_CONFIRM_WAIT"
            reasons.append("await_short_confirmation")
        elif ev >= 35:
            tier = "WATCH_SHORT"
            reasons.append("direction_consensus_short")
        else:
            tier = "EARLY_SHORT"
            reasons.append("early_short_bias")

    if ctx >= 28 and tier not in ("LONG_TRIGGER_READY", "SHORT_TRIGGER_READY"):
        if side == "long" and tier in ("LONG_RETEST_READY", "LONG_CONFIRM_WAIT"):
            tier = "WATCH_LONG"
            reasons.append("context_penalty")
        elif side == "short" and tier in ("SHORT_RETEST_READY", "SHORT_CONFIRM_WAIT"):
            tier = "WATCH_SHORT"
            reasons.append("context_penalty")

    return {
        "final_tier": tier,
        "final_tier_side": side,
        "final_tier_reasons": reasons[:4],
    }

def final_recommendation_v3(d):
    tier = d.get("final_tier", "WAIT")
    side = d.get("final_tier_side", "none")
    hold = d.get("decision_hold_preference", False)

    action = tier
    priority = "low"
    if tier in ("LONG_TRIGGER_READY", "SHORT_TRIGGER_READY"):
        priority = "high"
    elif tier in ("LONG_RETEST_READY", "SHORT_RETEST_READY", "LONG_CONFIRM_WAIT", "SHORT_CONFIRM_WAIT", "WATCH_LONG", "WATCH_SHORT"):
        priority = "medium"

    if hold and tier in ("WATCH_LONG", "WATCH_SHORT", "LONG_CONFIRM_WAIT", "SHORT_CONFIRM_WAIT"):
        priority = "medium"

    return {
        "final_action_v3": action,
        "final_side_v3": side,
        "final_priority_v3": priority,
        "final_reason_v3": d.get("final_tier_reasons", [])[:4],
    }


def final_recommendation_v4(d):
    direction = d.get("direction_consensus_side") or "neutral"
    score = d.get("setup_readiness_v2") or d.get("setup_readiness_score") or 0.0
    ev = d.get("expected_value_score") or 0.0
    timing = d.get("entry_timing_score") or 0.0
    context = d.get("context_penalty_score") or 0.0
    execution = d.get("execution_feasibility_score") or 0.0
    conflict = d.get("signal_conflict_score") or 0.0

    action = "WAIT"
    side = "none"
    priority = "low"
    reason = []

    if d.get("no_trade_active"):
        return {
            "final_action_v4": "NO_TRADE",
            "final_side_v4": "none",
            "final_priority_v4": "low",
            "final_reason_v4": (d.get("no_trade_reason") or [])[:4],
        }

    if conflict >= 55:
        return {
            "final_action_v4": "WAIT",
            "final_side_v4": "none",
            "final_priority_v4": "low",
            "final_reason_v4": ["signal_conflict"],
        }

    if direction == "long":
        side = "long"
        if d.get("bull_break_valid") and timing >= 65 and ev >= 58 and context < 28 and execution >= 55:
            action = "LONG_TRIGGER_READY"
            priority = "high"
            reason.append("bull_break_valid")
        elif d.get("retest_winner_side") == "long" and score >= 45 and ev >= 50:
            action = "LONG_RETEST_READY"
            priority = "medium"
            reason.append("long_retest_winner")
        elif score >= 38:
            action = "LONG_CONFIRM_WAIT"
            priority = "medium"
            reason.append("await_long_confirmation")
        else:
            action = "WATCH_LONG"
            priority = "medium"
            reason.append("direction_consensus_long")

    elif direction == "short":
        side = "short"
        if d.get("bear_break_valid") and timing >= 65 and ev >= 58 and context < 28 and execution >= 55:
            action = "SHORT_TRIGGER_READY"
            priority = "high"
            reason.append("bear_break_valid")
        elif d.get("retest_winner_side") == "short" and score >= 45 and ev >= 50:
            action = "SHORT_RETEST_READY"
            priority = "medium"
            reason.append("short_retest_winner")
        elif score >= 38:
            action = "SHORT_CONFIRM_WAIT"
            priority = "medium"
            reason.append("await_short_confirmation")
        else:
            action = "WATCH_SHORT"
            priority = "medium"
            reason.append("direction_consensus_short")

    return {
        "final_action_v4": action,
        "final_side_v4": side,
        "final_priority_v4": priority,
        "final_reason_v4": reason[:4],
    }


def evaluate_long_path(d):
    valid = d.get("direction_consensus_side") == "long"
    score = 0.0
    blockers = []

    if valid:
        score += min((d.get("soft_score_total") or 0.0) * 0.40, 36)
        score += min((d.get("trigger_behavior_long_score") or 0.0) * 0.22, 18)
        score += min((d.get("signal_freshness_score") or 0.0) * 0.18, 14)
        score += min((d.get("zone_proximity_long_score") or 0.0) * 0.12, 10)
        score += min((d.get("support_cluster_score") or 0.0) * 0.08, 8)
        score += 12 if d.get("retest_winner_side") == "long" else 0
        score += 12 if d.get("bull_break_valid_v2") else 0
        if d.get("failed_break_short"):
            score += min((d.get("failed_break_short_strength") or 0.0) * 0.18, 8)

    if not d.get("hard_gate_pass"):
        blockers.extend(d.get("hard_gate_blocks") or [])
    if not d.get("above_long_trigger_acceptance") and not d.get("bull_break_valid_v2"):
        blockers.append("need_long_acceptance")
    if d.get("zone_position_in_resistance") is not None and not d.get("above_long_trigger_acceptance"):
        zres = float(d.get("zone_position_in_resistance") or 0.0)
        blockers.append("inside_resistance_zone")
        score -= min(10.0 + zres * 16.0, 20.0)
        if zres >= 0.45:
            score -= 5.0
    if (d.get("distance_to_resistance_pct") is not None and (d.get("distance_to_resistance_pct") or 999.0) <= 0.03 and not d.get("bull_break_valid_v2")):
        blockers.append("too_close_to_resistance")
        score -= 8.0
    if (d.get("trigger_distance_pct") or 999.0) <= 0.02 and not d.get("above_long_trigger_acceptance"):
        blockers.append("trigger_too_close_noise")
        score -= 6.0
    if (d.get("long_level_fatigue_score") or 0) >= 28:
        blockers.append("long_level_fatigue")
    if (d.get("resistance_fragility_score") or 0) >= 72:
        blockers.append("resistance_fragile")

    return {
        "long_path_valid": valid,
        "long_path_score": round(min(max(score, 0), 100), 2),
        "long_path_blockers": blockers[:6],
    }

def evaluate_short_path(d):
    valid = d.get("direction_consensus_side") == "short"
    score = 0.0
    blockers = []

    if valid:
        score += min((d.get("soft_score_total") or 0.0) * 0.40, 36)
        score += min((d.get("trigger_behavior_short_score") or 0.0) * 0.22, 18)
        score += min((d.get("signal_freshness_score") or 0.0) * 0.18, 14)
        score += min((d.get("zone_proximity_short_score") or 0.0) * 0.12, 10)
        score += min((d.get("resistance_cluster_score") or 0.0) * 0.08, 8)
        score += 12 if d.get("retest_winner_side") == "short" else 0
        score += 12 if d.get("bear_break_valid_v2") else 0
        if d.get("failed_break_long"):
            score += min((d.get("failed_break_long_strength") or 0.0) * 0.18, 8)

    if not d.get("hard_gate_pass"):
        blockers.extend(d.get("hard_gate_blocks") or [])
    if not d.get("below_short_trigger_acceptance") and not d.get("bear_break_valid_v2"):
        blockers.append("need_short_acceptance")
    if d.get("zone_position_in_support") is not None and not d.get("below_short_trigger_acceptance"):
        zsup = float(d.get("zone_position_in_support") or 0.0)
        blockers.append("inside_support_zone")
        score -= min(10.0 + (1.0 - zsup) * 16.0, 20.0)
        if zsup <= 0.55:
            score -= 5.0
    if (d.get("distance_to_support_pct") is not None and (d.get("distance_to_support_pct") or 999.0) <= 0.03 and not d.get("bear_break_valid_v2")):
        blockers.append("too_close_to_support")
        score -= 8.0
    if (d.get("trigger_distance_pct") or 999.0) <= 0.02 and not d.get("below_short_trigger_acceptance"):
        blockers.append("trigger_too_close_noise")
        score -= 6.0
    if (d.get("short_level_fatigue_score") or 0) >= 28:
        blockers.append("short_level_fatigue")
    if (d.get("support_fragility_score") or 0) >= 72:
        blockers.append("support_fragile")

    return {
        "short_path_valid": valid,
        "short_path_score": round(min(max(score, 0), 100), 2),
        "short_path_blockers": blockers[:6],
    }

def final_path_decision(d):
    long_score = d.get("long_path_score") or 0.0
    short_score = d.get("short_path_score") or 0.0
    hold = d.get("decision_hold_preference", False)

    support_bias = (d.get("support_memory_score") or 0.0) * 0.08 + (d.get("support_bounce_quality_avg") or 0.0) * 0.06
    resistance_bias = (d.get("resistance_memory_score") or 0.0) * 0.08 + (d.get("resistance_bounce_quality_avg") or 0.0) * 0.06

    long_score += min((d.get("zone_proximity_long_score") or 0.0) * 0.10, 8.0)
    short_score += min((d.get("zone_proximity_short_score") or 0.0) * 0.10, 8.0)
    long_score += min((d.get("support_cluster_score") or 0.0) * 0.10, 8.0)
    short_score += min((d.get("resistance_cluster_score") or 0.0) * 0.10, 8.0)
    if d.get("failed_break_short"):
        long_score += min((d.get("failed_break_short_strength") or 0.0) * 0.20, 10.0)
    if d.get("failed_break_long"):
        short_score += min((d.get("failed_break_long_strength") or 0.0) * 0.20, 10.0)

    # Fine-tune with zone location so that longs weaken deeper in resistance and shorts weaken deeper in support.
    zres = d.get("zone_position_in_resistance")
    zsup = d.get("zone_position_in_support")
    if zres is not None:
        long_score -= min(10.0 + float(zres) * 14.0, 20.0)
        short_score += min(4.0 + float(zres) * 8.0, 10.0)
    if zsup is not None:
        short_score -= min(10.0 + (1.0 - float(zsup)) * 14.0, 20.0)
        long_score += min(4.0 + (1.0 - float(zsup)) * 8.0, 10.0)

    if d.get("active_tested_side") == "support":
        short_score -= min(support_bias, 18.0)
        short_score -= min((d.get("support_fragility_score") or 0.0) * 0.10, 10.0)
        short_score -= min((d.get("support_bounce_quality_avg") or 0.0) * 0.10, 8.0)
        long_score += min((d.get("support_hold_count") or 0) * 2.5 + (d.get("support_reclaim_count") or 0) * 3.0, 10.0)
        long_score += min((d.get("support_bounce_quality_avg") or 0.0) * 0.10, 8.0)
    elif d.get("active_tested_side") == "resistance":
        long_score -= min(resistance_bias, 18.0)
        long_score -= min((d.get("resistance_fragility_score") or 0.0) * 0.10, 10.0)
        long_score -= min(max(0.0, 55.0 - (d.get("resistance_bounce_quality_avg") or 0.0)) * 0.12, 6.0)
        short_score += min((d.get("resistance_reject_total") or 0) * 1.4, 10.0)
        short_score += min((d.get("resistance_bounce_quality_avg") or 0.0) * 0.10, 8.0)

    long_score = min(max(long_score, 0.0), 100.0)
    short_score = min(max(short_score, 0.0), 100.0)

    side = "none"
    action = "WAIT"
    reason = []

    if d.get("hard_gate_pass") is False:
        return {
            "final_path_side": "none",
            "final_path_action": "WAIT",
            "final_path_reason": (d.get("hard_gate_blocks") or [])[:5],
            "level_memory_long_adjusted_score": round(long_score, 2),
            "level_memory_short_adjusted_score": round(short_score, 2),
        }

    if long_score >= short_score + 8 and d.get("long_path_valid"):
        side = "long"
        if d.get("bull_break_valid_v2") and (d.get("long_acceptance_score") or 0) >= 55 and long_score >= 62:
            action = "LONG_TRIGGER_READY"
            reason.append("long_breakout_confirmed_v2")
        elif d.get("bull_break_valid") and long_score >= 62:
            action = "LONG_TRIGGER_READY"
            reason.append("long_path_confirmed")
        elif (d.get("retest_winner_side") == "long" or d.get("retest_long_ready")) and long_score >= 52:
            action = "LONG_RETEST_READY"
            reason.append("long_retest_path")
            if (d.get("active_tested_side") == "resistance" and (d.get("resistance_memory_score") or 0.0) >= 80.0 and (d.get("distance_to_resistance_pct") or 999.0) <= 0.03 and not d.get("bull_break_valid_v2")):
                action = "LONG_CONFIRM_WAIT"
                reason.append("resistance_needs_acceptance")
        elif long_score >= 42:
            action = "LONG_CONFIRM_WAIT"
            reason.append("long_path_needs_confirmation")
        else:
            action = "WATCH_LONG"
            reason.append("long_path_watch")
    elif short_score >= long_score + 8 and d.get("short_path_valid"):
        side = "short"
        if d.get("bear_break_valid_v2") and (d.get("short_acceptance_score") or 0) >= 55 and short_score >= 62:
            action = "SHORT_TRIGGER_READY"
            reason.append("short_breakout_confirmed_v2")
        elif d.get("bear_break_valid") and short_score >= 62:
            action = "SHORT_TRIGGER_READY"
            reason.append("short_path_confirmed")
        elif (d.get("retest_winner_side") == "short" or d.get("retest_short_ready")) and short_score >= 52:
            action = "SHORT_RETEST_READY"
            reason.append("short_retest_path")
            if (d.get("active_tested_side") == "support" and (d.get("support_memory_score") or 0.0) >= 80.0 and (d.get("distance_to_support_pct") or 999.0) <= 0.03 and not d.get("bear_break_valid_v2")):
                action = "SHORT_CONFIRM_WAIT"
                reason.append("support_needs_acceptance")
        elif short_score >= 42:
            action = "SHORT_CONFIRM_WAIT"
            reason.append("short_path_needs_confirmation")
        else:
            action = "WATCH_SHORT"
            reason.append("short_path_watch")

    if hold and action in ("WATCH_LONG", "WATCH_SHORT", "LONG_CONFIRM_WAIT", "SHORT_CONFIRM_WAIT"):
        reason.append("hysteresis_hold")

    nearest_side = d.get("nearest_zone_side") or "none"
    dist_res = d.get("distance_to_resistance_pct")
    dist_sup = d.get("distance_to_support_pct")
    if side == "short" and ((d.get("active_tested_side") == "support" and (d.get("support_memory_score") or 0) >= 70) or (nearest_side == "support" and dist_sup is not None and dist_sup <= 0.08) or _inside_support_without_accept(d)):
        reason.append("short_vs_strong_support")
        if action == "SHORT_RETEST_READY" and not d.get("below_short_trigger_acceptance"):
            action = "SHORT_CONFIRM_WAIT"
        if _inside_support_without_accept(d) and (d.get("zone_position_in_support") or 0.0) <= 0.40 and not d.get("bear_break_valid_v2"):
            action = "WATCH_SHORT"
    if side == "long" and ((d.get("active_tested_side") == "resistance" and (d.get("resistance_memory_score") or 0) >= 70) or (nearest_side == "resistance" and dist_res is not None and dist_res <= 0.08) or _inside_resistance_without_accept(d)):
        reason.append("long_vs_strong_resistance")
        if action == "LONG_RETEST_READY" and not d.get("above_long_trigger_acceptance"):
            action = "LONG_CONFIRM_WAIT"
        if _inside_resistance_without_accept(d) and (d.get("zone_position_in_resistance") or 0.0) >= 0.60 and not d.get("bull_break_valid_v2"):
            action = "WATCH_LONG"

    return {
        "final_path_side": side,
        "final_path_action": action,
        "final_path_reason": reason[:5],
        "level_memory_long_adjusted_score": round(long_score, 2),
        "level_memory_short_adjusted_score": round(short_score, 2),
    }




def _inside_resistance_without_accept(d):
    zres = d.get("zone_position_in_resistance")
    if zres is None:
        return False
    return (
        float(zres) >= 0.35
        and not bool(d.get("above_long_trigger_acceptance"))
        and not bool(d.get("bull_break_valid_v2"))
        and (float(d.get("long_acceptance_score") or 0.0) < 55.0)
    )


def _inside_support_without_accept(d):
    zsup = d.get("zone_position_in_support")
    if zsup is None:
        return False
    return (
        float(zsup) <= 0.65
        and not bool(d.get("below_short_trigger_acceptance"))
        and not bool(d.get("bear_break_valid_v2"))
        and (float(d.get("short_acceptance_score") or 0.0) < 55.0)
    )


def harmonize_final_outputs(d):
    path_action = d.get("final_path_action") or "WAIT"
    path_side = d.get("final_path_side") or "none"
    path_reason = list(d.get("final_path_reason") or [])

    hard_gate_pass = bool(d.get("hard_gate_pass", True))
    no_trade_active = bool(d.get("no_trade_active"))
    support_memory = float(d.get("support_memory_score") or 0.0)
    resistance_memory = float(d.get("resistance_memory_score") or 0.0)
    active_side = d.get("active_tested_side") or "none"

    long_accept = float(d.get("long_acceptance_score") or 0.0)
    short_accept = float(d.get("short_acceptance_score") or 0.0)
    bull_valid = bool(d.get("bull_break_valid_v2"))
    bear_valid = bool(d.get("bear_break_valid_v2"))
    above_accept = bool(d.get("above_long_trigger_acceptance"))
    below_accept = bool(d.get("below_short_trigger_acceptance"))

    out = {}

    def set_out(action, side, priority, reasons):
        out["final_action_v4"] = action
        out["final_side_v4"] = side
        out["final_priority_v4"] = priority
        out["final_reason_v4"] = list(reasons)[:4] if reasons else [action.lower()]
        return out

    # Hard gate wins first.
    if not hard_gate_pass:
        return set_out("NO_TRADE", "none", "low", d.get("hard_gate_blocks") or ["hard_gate_block"])

    # Global no-trade filter wins unless the internal path already reached a true trigger-ready state.
    if no_trade_active and path_action not in ("LONG_TRIGGER_READY", "SHORT_TRIGGER_READY"):
        return set_out("NO_TRADE", "none", "low", d.get("no_trade_reason") or ["no_trade_filter_active"])

    # If the path itself already resolved to confirm-wait / watch / wait, preserve that exactly.
    preserve_map = {
        "WAIT": ("WAIT", "none", "low"),
        "NO_TRADE_CHOP": ("NO_TRADE", "none", "low"),
        "NO_TRADE_LOW_LIQUIDITY": ("NO_TRADE", "none", "low"),
        "NO_TRADE_CONFLICT": ("NO_TRADE", "none", "low"),
        "WATCH_LONG": ("WATCH_LONG", "long", "medium"),
        "WATCH_SHORT": ("WATCH_SHORT", "short", "medium"),
        "LONG_WEAK_CONTEXT": ("WATCH_LONG", "long", "medium"),
        "SHORT_WEAK_CONTEXT": ("WATCH_SHORT", "short", "medium"),
        "LONG_CONFIRM_WAIT": ("LONG_CONFIRM_WAIT", "long", "medium"),
        "SHORT_CONFIRM_WAIT": ("SHORT_CONFIRM_WAIT", "short", "medium"),
        "LONG_TRIGGER_READY": ("LONG_TRIGGER_READY", "long", "high"),
        "SHORT_TRIGGER_READY": ("SHORT_TRIGGER_READY", "short", "high"),
        "NO_TRADE": ("NO_TRADE", "none", "low"),
    }
    if path_action in preserve_map:
        action, side, priority = preserve_map[path_action]
        return set_out(action, side, priority, path_reason)

    # Retest-ready is an internal path state.
    # User-facing v4 should normally show CONFIRM_WAIT until both validation and acceptance are truly present.
    if path_action == "LONG_RETEST_READY":
        # Strong nearby resistance with no real acceptance => keep it conservative.
        if (
            (active_side == "resistance" and resistance_memory >= 75.0)
            or ((d.get("nearest_zone_side") == "resistance") and ((d.get("distance_to_resistance_pct") or 999) <= 0.10))
            or _inside_resistance_without_accept(d)
        ) and (not bull_valid or not above_accept or long_accept < 55.0):
            if (d.get("zone_position_in_resistance") or 0.0) >= 0.60 and not bull_valid:
                return set_out("WATCH_LONG", "long", "medium", ["await_long_confirmation"])
            return set_out("LONG_CONFIRM_WAIT", "long", "medium", ["await_long_confirmation"])

        if bull_valid and above_accept and long_accept >= 55.0:
            return set_out("LONG_TRIGGER_READY", "long", "high", ["long_breakout_confirmed_v2"])

        return set_out("LONG_CONFIRM_WAIT", "long", "medium", ["await_long_confirmation"])

    if path_action == "SHORT_RETEST_READY":
        # Strong nearby support with no real acceptance => keep it conservative.
        if (
            (active_side == "support" and support_memory >= 75.0)
            or ((d.get("nearest_zone_side") == "support") and ((d.get("distance_to_support_pct") or 999) <= 0.10))
            or _inside_support_without_accept(d)
        ) and (not bear_valid or not below_accept or short_accept < 55.0):
            if (d.get("zone_position_in_support") or 1.0) <= 0.40 and not bear_valid:
                return set_out("WATCH_SHORT", "short", "medium", ["await_short_confirmation"])
            return set_out("SHORT_CONFIRM_WAIT", "short", "medium", ["await_short_confirmation"])

        if bear_valid and below_accept and short_accept >= 55.0:
            return set_out("SHORT_TRIGGER_READY", "short", "high", ["short_breakout_confirmed_v2"])

        return set_out("SHORT_CONFIRM_WAIT", "short", "medium", ["await_short_confirmation"])

    # Fallback safety: unknown internal states should not leak through to v4.
    return set_out("WAIT", "none", "low", path_reason or ["unresolved_final_state"])

def final_decision_v4(d):
    return harmonize_final_outputs(d)
