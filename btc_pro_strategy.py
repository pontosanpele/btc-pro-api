from datetime import datetime
from zoneinfo import ZoneInfo
from btc_pro_config import SYMBOL_PERP, clamp
from btc_pro_history import append_history, compare_to_previous, load_history
from btc_pro_metrics import normalized_market_context, interaction_scores
from btc_pro_market import all_oi,funding_history,global_data,instrument_info,liquidation_tracker_best_effort,orderbook,recent_trades,spot_perp_divergence,ticker,volume_and_structure
from btc_pro_signals import chop_filter,multi_tf_trend,delta_divergence_detector,absorption_detector,flow_metrics,volume_quality
from btc_pro_levels import liquidity_map_proxy,orderbook_wall_tracker,trigger_engine,trigger_acceptance,retest_detector
from btc_pro_validation import (
    breakout_quality, breakout_validator, rr_metrics, trap_detector, exhaustion_detector,
    rejection_detector, orderflow_consistency, execution_realism, entry_timing_score,
    no_trade_filter, classify_no_trade_context, confidence_decomposition,
    execution_feasibility_score, expected_value_score, decision_drivers,
    context_penalty_score, invalidation_quality,
    regime_weighted_composite, recalibrated_breakout_quality, recalibrated_setup_readiness,
    trigger_requirements, trigger_behavior_score, signal_freshness_score,
    level_fatigue_score, market_vs_trade_read, level_memory_features,
    acceptance_engine, breakout_validation_v2, zone_proximity_features,
    zone_cluster_scores, zone_fragility_features, failed_break_detector,
)
from btc_pro_bias import (
    compute_scores, signal_agreement, setup_readiness, alert_priority,
    bias_confirmation, direction_consensus, signal_conflict_detector, compute_scores_v2,
    market_and_trading_bias, hard_gate_evaluation, soft_score_stack,
)
from btc_pro_state import (
    detect_regime, reversal_probability, trade_lifecycle_state, setup_classifier,
    summary_generator, state_machine_v2, final_recommendation, state_change_alerts,
    hierarchical_decision_engine, final_recommendation_v2,
    decision_hysteresis_v2, final_judgment_tiers, final_recommendation_v3, final_recommendation_v4,
    evaluate_long_path, evaluate_short_path, final_path_decision,
)

def session_context(now_dt):
    hour=now_dt.hour
    if now_dt.weekday()>=5: return {'session_name':'weekend','session_liquidity_profile':'thin'}
    if 0<=hour<7: return {'session_name':'asia','session_liquidity_profile':'medium'}
    if 7<=hour<13: return {'session_name':'europe','session_liquidity_profile':'medium_high'}
    if 13<=hour<21: return {'session_name':'us','session_liquidity_profile':'high'}
    return {'session_name':'after_hours','session_liquidity_profile':'medium_low'}

def data_quality(snapshot):
    missing=[]; btc=snapshot.get('btc',{})
    for k in ['last','funding_pct','oi_change_5m_pct','recent_notional_delta_pct','volume_spike_5m_x','orderbook_imbalance_0_25_pct','vwap_1h']:
        if btc.get(k) is None: missing.append(k)
    score=100-len(missing)*8
    if btc.get('liq_available') is False: score -= 10
    return {'data_quality_score':max(score,0),'missing_modules':missing,'ws_liq_status':'ok' if btc.get('liq_available') else 'missing_or_inactive'}

def early_setup_detector(d):
    long_near=short_near=0.0; alert='none'; last=d.get('last'); bull=d.get('bull_trigger_price'); bear=d.get('bear_trigger_price')
    if None not in (last,bull) and last!=0:
        dist=abs(bull-last)/last*100.0; long_near += 35 if dist<=0.35 else 20 if dist<=0.60 else 0
    if None not in (last,bear) and last!=0:
        dist=abs(last-bear)/last*100.0; short_near += 35 if dist<=0.35 else 20 if dist<=0.60 else 0
    if d.get('trend_1h')=='up' and (d.get('price_vs_vwap_1h_pct') or 0)>0 and (d.get('cvd_trend_usd') or 0)>0: long_near += 18
    if d.get('trend_1h')=='down' and (d.get('price_vs_vwap_1h_pct') or 0)<0 and (d.get('cvd_trend_usd') or 0)<0: short_near += 18
    long_near += (d.get('signal_agreement_long') or 0)*5; short_near += (d.get('signal_agreement_short') or 0)*5
    long_near=round(clamp(long_near,0,100),2); short_near=round(clamp(short_near,0,100),2)
    if long_near>=55 and long_near>short_near: alert='possible_long_setup_near'
    elif short_near>=55 and short_near>long_near: alert='possible_short_setup_near'
    return {'early_setup_score_long':long_near,'early_setup_score_short':short_near,'early_setup_alert':alert}

def trade_plan_generator(d):
    side=d.get('trade_bias'); cancel=[]
    if d.get('no_trade_active'): side='no_trade'; cancel.append('no_trade_filter_active')
    if d.get('trap_alert')=='bull_trap_risk' and side=='long': side='no_trade'; cancel.append('bull_trap_risk')
    if d.get('trap_alert')=='bear_trap_risk' and side=='short': side='no_trade'; cancel.append('bear_trap_risk')
    if side=='long':
        return {'trade_plan_side':side,'trade_plan_entry_zone':[d.get('bull_trigger_price'),d.get('bull_trigger_price')],'trade_plan_stop':d.get('atr_stop_long'),'trade_plan_t1':d.get('target_long_1'),'trade_plan_t2':d.get('target_long_2'),'trade_plan_cancel_if':cancel+['lose_bull_trigger'],'trade_plan_confidence':d.get('confidence_score')}
    if side=='short':
        return {'trade_plan_side':side,'trade_plan_entry_zone':[d.get('bear_trigger_price'),d.get('bear_trigger_price')],'trade_plan_stop':d.get('atr_stop_short'),'trade_plan_t1':d.get('target_short_1'),'trade_plan_t2':d.get('target_short_2'),'trade_plan_cancel_if':cancel+['reclaim_bear_trigger'],'trade_plan_confidence':d.get('confidence_score')}
    return {'trade_plan_side':'no_trade','trade_plan_entry_zone':None,'trade_plan_stop':None,'trade_plan_t1':None,'trade_plan_t2':None,'trade_plan_cancel_if':cancel,'trade_plan_confidence':d.get('confidence_score')}

def build_snapshot():
    now = datetime.now(ZoneInfo('Europe/Bucharest'))
    out = {
        'ts_bucharest': now.strftime('%Y-%m-%d %H:%M:%S'),
        'weekend_flag': now.weekday() >= 5,
        'instrument': instrument_info(),
    }
    btc = {}

    # Market data
    btc.update(ticker('linear', SYMBOL_PERP))
    btc.update(funding_history())
    btc.update(all_oi())
    btc.update(orderbook('linear', SYMBOL_PERP, 200))
    btc.update(recent_trades('linear', SYMBOL_PERP, 1000))
    btc.update(volume_and_structure('linear', SYMBOL_PERP))
    btc.update(spot_perp_divergence())
    btc.update(liquidation_tracker_best_effort(duration_sec=8))
    btc.update(session_context(now))

    # Signals
    btc.update(chop_filter(btc))
    btc.update(multi_tf_trend(btc))
    btc.update(delta_divergence_detector(btc))
    btc.update(absorption_detector(btc))
    btc.update(flow_metrics(btc))
    btc.update(volume_quality(btc))

    # Levels
    btc.update(liquidity_map_proxy(btc))
    btc.update(orderbook_wall_tracker(btc))
    btc.update(trigger_engine(btc))
    btc.update(trigger_acceptance(btc))
    btc.update(retest_detector(btc))

    # Regime / validation
    btc['market_regime'] = detect_regime(btc)
    btc.update(reversal_probability(btc))
    btc.update(breakout_quality(btc))
    btc.update(breakout_validator(btc))
    btc.update(rr_metrics(btc))
    btc.update(trap_detector(btc))
    btc.update(exhaustion_detector(btc))
    btc.update(rejection_detector(btc))
    btc.update(orderflow_consistency(btc))

    # Scores + history baseline
    btc.update(compute_scores(btc))
    hist = load_history()
    prev = compare_to_previous({'btc': btc}, hist)
    btc.update(prev)

    # Decision core
    btc.update(normalized_market_context(btc, hist))
    btc.update(interaction_scores(btc))
    btc.update(signal_agreement(btc))
    btc.update(setup_readiness(btc))
    btc.update(early_setup_detector(btc))
    btc.update(alert_priority(btc))
    btc.update(bias_confirmation(btc))
    btc.update(direction_consensus(btc))
    btc.update(signal_conflict_detector(btc))

    # Execution / filters / judgment
    btc.update(execution_realism(btc))
    btc.update(entry_timing_score(btc))
    btc.update(confidence_decomposition(btc))
    btc.update(context_penalty_score(btc))
    btc.update(invalidation_quality(btc))
    btc.update(execution_feasibility_score(btc))
    btc.update(regime_weighted_composite(btc))
    btc.update(recalibrated_breakout_quality(btc))
    btc.update(expected_value_score(btc))
    btc.update(no_trade_filter(btc))
    btc.update(classify_no_trade_context(btc))
    btc.update(recalibrated_setup_readiness(btc))
    btc.update(compute_scores_v2(btc))
    btc.update(decision_drivers(btc))
    btc.update(trigger_requirements(btc))
    btc.update(trigger_behavior_score(btc))
    btc.update(signal_freshness_score(btc))
    btc.update(level_memory_features(btc, hist))
    btc.update(zone_proximity_features(btc))
    btc.update(zone_cluster_scores(btc))
    btc.update(zone_fragility_features(btc))
    btc.update(level_fatigue_score(btc))
    btc.update(acceptance_engine(btc, hist))
    btc.update(failed_break_detector(btc, hist))
    btc.update(breakout_validation_v2(btc))
    btc.update(market_and_trading_bias(btc))
    btc.update(hard_gate_evaluation(btc))
    btc.update(soft_score_stack(btc))
    btc.update(market_vs_trade_read(btc))

    # State + final outputs
    btc.update(trade_lifecycle_state(btc))
    btc.update(setup_classifier(btc))
    btc.update(state_machine_v2(btc))
    btc.update(trade_plan_generator(btc))
    btc.update(summary_generator(btc))
    btc.update(hierarchical_decision_engine(btc))
    btc.update(decision_hysteresis_v2(btc))
    btc.update(final_judgment_tiers(btc))
    btc.update(final_recommendation(btc))
    btc.update(final_recommendation_v2(btc))
    btc.update(final_recommendation_v3(btc))
    btc.update(final_recommendation_v4(btc))
    btc.update(evaluate_long_path(btc))
    btc.update(evaluate_short_path(btc))
    btc.update(final_path_decision(btc))

    out['btc'] = btc
    out['global'] = global_data()
    out.update(data_quality(out))

    metrics = compare_to_previous(out, hist)
    out.update(metrics)
    out['btc'].update(metrics)
    out['btc'].update(state_change_alerts(out['btc']))

    append_history(out)
    return out
