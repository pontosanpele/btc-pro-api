from datetime import datetime
from zoneinfo import ZoneInfo

from btc_pro_config import SYMBOL_PERP, clamp
from btc_pro_history import append_history, compare_to_previous, load_history
from btc_pro_metrics import normalized_market_context, interaction_scores
from btc_pro_market import (
    all_oi, funding_history, global_data, instrument_info,
    liquidation_tracker, orderbook, recent_trades,
    spot_perp_divergence, ticker, volume_and_structure,
)
from btc_pro_signals import (
    chop_filter, multi_tf_trend, delta_divergence_detector,
    absorption_detector, flow_metrics, volume_quality,
)
from btc_pro_levels import (
    liquidity_map_proxy, orderbook_wall_tracker,
    trigger_engine, trigger_acceptance, retest_detector,
)
from btc_pro_validation import (
    breakout_quality, breakout_validator, rr_metrics, trap_detector,
    exhaustion_detector, rejection_detector, orderflow_consistency,
    execution_realism, entry_timing_score, no_trade_filter,
    classify_no_trade_context, confidence_decomposition,
    execution_feasibility_score, expected_value_score, decision_drivers,
    context_penalty_score, invalidation_quality, regime_weighted_composite,
    recalibrated_breakout_quality, recalibrated_setup_readiness,
    trigger_requirements, trigger_behavior_score, signal_freshness_score,
    level_fatigue_score, market_vs_trade_read, level_memory_features,
    acceptance_engine, breakout_validation_v2, zone_proximity_features,
    zone_cluster_scores, zone_fragility_features, failed_break_detector,
)
from btc_pro_bias import (
    compute_scores, signal_agreement, setup_readiness, alert_priority,
    bias_confirmation, direction_consensus, signal_conflict_detector,
    compute_scores_v2, market_and_trading_bias, hard_gate_evaluation,
    soft_score_stack, dominant_htf_bias, execution_bias_ltf,
)
from btc_pro_state import (
    detect_regime, reversal_probability, trade_lifecycle_state,
    setup_classifier, summary_generator, state_machine_v2,
    final_recommendation, state_change_alerts, hierarchical_decision_engine,
    final_recommendation_v2, decision_hysteresis_v2,
    final_judgment_tiers, final_recommendation_v3,
    final_recommendation_v4, evaluate_long_path, evaluate_short_path,
    final_path_decision, canonical_final_decision,
)

RETEST_READY_ACTIONS = {
    'SHORT_RETEST_READY',
    'LONG_RETEST_READY',
}


def _f(v):
    try:
        return float(v)
    except Exception:
        return None


def _zone(z):
    if isinstance(z, (list, tuple)) and len(z) >= 2 and z[0] is not None and z[1] is not None:
        lo = round(min(float(z[0]), float(z[1])), 2)
        hi = round(max(float(z[0]), float(z[1])), 2)
        return [lo, hi]
    return None


def _coalesce(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


def session_context(now_dt):
    hour = now_dt.hour
    if now_dt.weekday() >= 5:
        return {'session_name': 'weekend', 'session_liquidity_profile': 'thin'}
    if 0 <= hour < 7:
        return {'session_name': 'asia', 'session_liquidity_profile': 'medium'}
    if 7 <= hour < 13:
        return {'session_name': 'europe', 'session_liquidity_profile': 'medium_high'}
    if 13 <= hour < 21:
        return {'session_name': 'us', 'session_liquidity_profile': 'high'}
    return {'session_name': 'after_hours', 'session_liquidity_profile': 'medium_low'}


def data_quality(snapshot):
    missing = []
    btc = snapshot.get('btc', {})
    global_ctx = snapshot.get('global', {})
    for k in ['last', 'funding_pct', 'oi_change_5m_pct', 'recent_notional_delta_pct', 'volume_spike_5m_x', 'orderbook_imbalance_0_25_pct', 'vwap_1h']:
        if btc.get(k) is None:
            missing.append(k)

    score = 100 - len(missing) * 8
    if btc.get('liq_available') is False:
        score -= 10

    global_available = snapshot.get('global_data_available')
    if global_available is None:
        global_available = (global_ctx.get('source_global_status') == 'ok')
    if not global_available:
        score -= 5

    return {
        'data_quality_score': max(score, 0),
        'missing_modules': missing,
        'ws_liq_status': 'ok' if btc.get('liq_available') else 'missing_or_inactive',
        'global_data_status': 'ok' if global_available else 'degraded',
    }


def early_setup_detector(d):
    long_near = short_near = 0.0
    alert = 'none'
    last = _f(d.get('last'))
    bull = _f(d.get('bull_trigger_price'))
    bear = _f(d.get('bear_trigger_price'))
    if None not in (last, bull) and last != 0:
        dist = abs(bull - last) / last * 100.0
        long_near += 35 if dist <= 0.35 else 20 if dist <= 0.60 else 0
    if None not in (last, bear) and last != 0:
        dist = abs(last - bear) / last * 100.0
        short_near += 35 if dist <= 0.35 else 20 if dist <= 0.60 else 0
    if d.get('trend_1h') == 'up' and (d.get('price_vs_vwap_1h_pct') or 0) > 0 and (d.get('cvd_trend_usd') or 0) > 0:
        long_near += 18
    if d.get('trend_1h') == 'down' and (d.get('price_vs_vwap_1h_pct') or 0) < 0 and (d.get('cvd_trend_usd') or 0) < 0:
        short_near += 18
    long_near += (d.get('signal_agreement_long') or 0) * 5
    short_near += (d.get('signal_agreement_short') or 0) * 5
    long_near = round(clamp(long_near, 0, 100), 2)
    short_near = round(clamp(short_near, 0, 100), 2)
    if long_near >= 55 and long_near > short_near:
        alert = 'possible_long_setup_near'
    elif short_near >= 55 and short_near > long_near:
        alert = 'possible_short_setup_near'
    return {'early_setup_score_long': long_near, 'early_setup_score_short': short_near, 'early_setup_alert': alert}


def directional_entry_zones(d):
    bull = _f(d.get('bull_trigger_price'))
    bear = _f(d.get('bear_trigger_price'))
    support_lo = _f(d.get('support_zone_low'))
    support_hi = _f(d.get('support_zone_high'))
    resistance_lo = _f(d.get('resistance_zone_low'))
    resistance_hi = _f(d.get('resistance_zone_high'))
    resistance_ctr = _f(d.get('resistance_zone_center'))
    support_ctr = _f(d.get('support_zone_center'))
    bid = _f(d.get('largest_bid_wall_price'))
    ask = _f(d.get('largest_ask_wall_price'))
    vwap = _f(d.get('vwap_1h'))
    atr5 = max(_f(d.get('atr_5m')) or 0.0, 40.0)
    width = max(atr5 * 0.18, 20.0)

    long_counter = d.get('dominant_bias_htf') == 'short'
    short_counter = d.get('dominant_bias_htf') == 'long'

    long_main = long_aggr = long_cons = None
    if bull is not None:
        lo = _coalesce(max(x for x in [support_lo, bid, bull - width] if x is not None), bull - width) if any(x is not None for x in [support_lo, bid, bull - width]) else bull - width
        hi = bull
        if support_hi is not None:
            lo = max(lo, min(support_hi, bull))
        if long_counter:
            lo = max(lo, bull - width * 0.75)
        long_main = _zone([lo, hi])
        if long_main:
            span = long_main[1] - long_main[0]
            long_aggr = _zone([long_main[0], long_main[0] + span * 0.72])
            long_cons = _zone([long_main[0] + span * 0.48, long_main[1]])

    short_main = short_aggr = short_cons = None
    if bear is not None:
        aggressive_hi = min(x for x in [bear + width, support_hi, bear + atr5 * 0.22] if x is not None) if any(x is not None for x in [bear + width, support_hi, bear + atr5 * 0.22]) else bear + width
        short_aggr = _zone([bear, aggressive_hi])

        cons_lo = max(x for x in [vwap, resistance_lo, bear + width * 0.9, ask and ask - width * 0.25] if x is not None) if any(x is not None for x in [vwap, resistance_lo, bear + width * 0.9, ask and ask - width * 0.25]) else bear + width * 0.9
        cons_hi = min(x for x in [ask, resistance_ctr, resistance_hi, bear + width * 1.6] if x is not None) if any(x is not None for x in [ask, resistance_ctr, resistance_hi, bear + width * 1.6]) else bear + width * 1.6
        if cons_lo > cons_hi:
            cons_lo = bear + width * 0.6
            cons_hi = bear + width * 1.1
        short_cons = _zone([cons_lo, cons_hi])

        short_main = short_cons or short_aggr
        if short_main and long_cons and short_main[0] <= long_cons[1]:
            shift = max(width * 0.2, 8.0)
            short_cons = _zone([max(short_cons[0], long_cons[1] + shift), max(short_cons[1], long_cons[1] + shift * 1.4)]) if short_cons else short_cons
            short_main = short_cons or short_aggr

    return {
        'long_entry_zone': long_main,
        'short_entry_zone': short_main,
        'long_entry_zone_aggressive': long_aggr,
        'long_entry_zone_conservative': long_cons,
        'short_entry_zone_aggressive': short_aggr,
        'short_entry_zone_conservative': short_cons,
        'long_is_countertrend': long_counter,
        'short_is_countertrend': short_counter,
    }


def _derive_analysis_bias(d):
    for key in ('trade_bias', 'trading_bias', 'raw_trade_bias_v2', 'raw_trade_bias', 'direction_consensus_side', 'dominant_bias_htf'):
        v = d.get(key)
        if v in ('long', 'short'):
            return v
    return 'no_trade'


def _derive_execution_status(d):
    if d.get('trade_plan_invalidated'):
        return 'invalidated'
    if d.get('trade_plan_side') in ('long', 'short') and d.get('trade_plan_entry_zone'):
        return 'ready'
    if d.get('canonical_final_action') and 'WATCH' in str(d.get('canonical_final_action')):
        return 'watching'
    if d.get('retest_long_ready') or d.get('retest_short_ready'):
        return 'watching'
    return 'inactive'


def _derive_trade_plan_status(d):
    if d.get('trade_plan_invalidated'):
        return 'invalidated'
    if d.get('trade_plan_side') in ('long', 'short') and d.get('trade_plan_entry_zone'):
        return 'active'
    return 'inactive'


def _derive_canonical(d):
    got = d.get('canonical_final_action')
    if got:
        return {
            'canonical_final_action': got,
            'canonical_final_side': d.get('canonical_final_side'),
            'canonical_final_reason': d.get('canonical_final_reason') or [],
        }
    return canonical_final_decision(d)


def build_trade_report(d):
    canonical = _derive_canonical(d)
    analysis_bias = d.get('analysis_bias') or _derive_analysis_bias(d)
    execution_status = d.get('execution_status') or _derive_execution_status(d)
    trade_plan_status = d.get('trade_plan_status') or _derive_trade_plan_status(d)
    direction = d.get('trade_plan_side') if d.get('trade_plan_side') in ('long', 'short') else analysis_bias if analysis_bias in ('long', 'short') else d.get('direction_consensus_side', 'none')
    verdict = canonical.get('canonical_final_action') or d.get('final_tier') or d.get('final_action_v4') or d.get('final_action') or (str(direction).upper() if direction in ('long', 'short') else 'WAIT')
    report = {
        'direction': direction if direction in ('long', 'short') else 'no_trade',
        'analysis_bias': analysis_bias,
        'execution_status': execution_status,
        'trade_plan_status': trade_plan_status,
        'dominant_bias_htf': d.get('dominant_bias_htf'),
        'dominant_bias_context': d.get('dominant_bias_context'),
        'execution_bias_ltf': d.get('execution_bias_ltf'),
        'execution_bias_context': d.get('execution_bias_context'),
        'canonical_final_action': canonical.get('canonical_final_action'),
        'canonical_final_side': canonical.get('canonical_final_side'),
        'canonical_final_reason': canonical.get('canonical_final_reason'),
        'orderbook_wall_explanation': d.get('orderbook_wall_explanation'),
        'micro_orderbook_pressure': d.get('micro_orderbook_pressure'),
        'major_wall_pressure': d.get('major_wall_pressure'),
        'wall_pressure_side': d.get('wall_pressure_side'),
        'long_entry_zone': d.get('long_entry_zone'),
        'long_entry_zone_aggressive': d.get('long_entry_zone_aggressive'),
        'long_entry_zone_conservative': d.get('long_entry_zone_conservative'),
        'long_countertrend': d.get('long_is_countertrend'),
        'long_sl': d.get('atr_stop_long'),
        'long_tp1': d.get('target_long_1'),
        'long_tp2': d.get('target_long_2'),
        'short_entry_zone': d.get('short_entry_zone'),
        'short_entry_zone_aggressive': d.get('short_entry_zone_aggressive'),
        'short_entry_zone_conservative': d.get('short_entry_zone_conservative'),
        'short_countertrend': d.get('short_is_countertrend'),
        'short_sl': d.get('atr_stop_short'),
        'short_tp1': d.get('target_short_1'),
        'short_tp2': d.get('target_short_2'),
        'setup_grade': 'B',
        'verdict': verdict,
    }
    return {'analysis_bias': analysis_bias, 'execution_status': execution_status, 'trade_plan_status': trade_plan_status, **canonical, 'trade_report': report}


def _trade_plan_confidence(d, side):
    direction = _f(d.get('confidence_direction') or d.get('confidence_score') or 0) or 0.0
    execution = _f(d.get('confidence_execution') or d.get('execution_feasibility_score') or 0) or 0.0
    rr = _f(d.get('confidence_rr') or 0) or 0.0
    external = _f(d.get('confidence_external') or d.get('external_confirmation_score') or 0) or 0.0
    ev = _f(d.get('expected_value_score') or 0) or 0.0
    feasibility = _f(d.get('execution_feasibility_score') or 0) or 0.0
    signal_conflict = _f(d.get('signal_conflict_score') or 0) or 0.0
    penalty = _f(d.get('context_penalty_score') or 0) or 0.0
    late = _f(d.get('late_entry_risk') or 0) or 0.0
    base = direction * 0.22 + execution * 0.22 + rr * 0.16 + external * 0.06 + ev * 0.18 + feasibility * 0.16
    base -= signal_conflict * 0.12
    base -= penalty * 0.18
    base -= late * 0.12
    if d.get('canonical_final_action') in RETEST_READY_ACTIONS:
        base += 8
    if side == 'short' and d.get('short_path_valid'):
        base += 6
    if side == 'long' and d.get('long_path_valid'):
        base += 6
    if d.get('trade_plan_invalidated'):
        base -= 15
    return round(clamp(base, 0, 100), 2)


def trade_plan_generator(d):
    side = d.get('trade_bias')
    cancel = []
    if d.get('trade_plan_invalidated'):
        side = 'no_trade'
        cancel.append('trade_plan_invalidated')
    if d.get('no_trade_active'):
        side = 'no_trade'
        cancel.append('no_trade_filter_active')
    if d.get('trap_alert') == 'bull_trap_risk' and side == 'long':
        side = 'no_trade'; cancel.append('bull_trap_risk')
    if d.get('trap_alert') == 'bear_trap_risk' and side == 'short':
        side = 'no_trade'; cancel.append('bear_trap_risk')

    if side == 'long':
        zone = d.get('long_entry_zone_aggressive') or d.get('long_entry_zone')
        return {
            'trade_plan_side': 'long',
            'trade_plan_entry_zone': zone,
            'trade_plan_stop': d.get('atr_stop_long'),
            'trade_plan_t1': d.get('target_long_1'),
            'trade_plan_t2': d.get('target_long_2'),
            'trade_plan_cancel_if': cancel + ['lose_bull_trigger'],
            'trade_plan_confidence': _trade_plan_confidence(d, 'long'),
        }
    if side == 'short':
        zone = d.get('short_entry_zone_aggressive') or d.get('short_entry_zone')
        return {
            'trade_plan_side': 'short',
            'trade_plan_entry_zone': zone,
            'trade_plan_stop': d.get('atr_stop_short'),
            'trade_plan_t1': d.get('target_short_1'),
            'trade_plan_t2': d.get('target_short_2'),
            'trade_plan_cancel_if': cancel + ['reclaim_bear_trigger'],
            'trade_plan_confidence': _trade_plan_confidence(d, 'short'),
        }
    return {
        'trade_plan_side': 'no_trade',
        'trade_plan_entry_zone': None,
        'trade_plan_stop': None,
        'trade_plan_t1': None,
        'trade_plan_t2': None,
        'trade_plan_cancel_if': cancel,
        'trade_plan_confidence': _trade_plan_confidence(d, 'no_trade'),
    }


def build_snapshot():
    now = datetime.now(ZoneInfo('Europe/Bucharest'))
    out = {
        'ts_bucharest': now.strftime('%Y-%m-%d %H:%M:%S'),
        'weekend_flag': now.weekday() >= 5,
        'instrument': instrument_info(),
    }
    btc = {}
    btc.update(ticker('linear', SYMBOL_PERP))
    btc.update(funding_history())
    btc.update(all_oi())
    btc.update(orderbook('linear', SYMBOL_PERP, 200))
    btc.update(recent_trades('linear', SYMBOL_PERP, 1000))
    btc.update(volume_and_structure('linear', SYMBOL_PERP))
    btc.update(spot_perp_divergence())
    btc.update(liquidation_tracker(window_sec=300))
    btc.update(session_context(now))

    btc.update(chop_filter(btc))
    btc.update(multi_tf_trend(btc))
    btc.update(dominant_htf_bias(btc))
    btc.update(delta_divergence_detector(btc))
    btc.update(absorption_detector(btc))
    btc.update(flow_metrics(btc))
    btc.update(volume_quality(btc))
    btc.update(execution_bias_ltf(btc))

    btc.update(liquidity_map_proxy(btc))
    btc.update(orderbook_wall_tracker(btc))
    btc.update(trigger_engine(btc))
    btc.update(trigger_acceptance(btc))
    btc.update(retest_detector(btc))

    btc['market_regime'] = detect_regime(btc)
    btc.update(reversal_probability(btc))
    btc.update(breakout_quality(btc))
    btc.update(breakout_validator(btc))
    btc.update(rr_metrics(btc))
    btc.update(trap_detector(btc))
    btc.update(exhaustion_detector(btc))
    btc.update(rejection_detector(btc))
    btc.update(orderflow_consistency(btc))

    btc.update(compute_scores(btc))
    hist = load_history()
    prev = compare_to_previous({'btc': btc}, hist)
    btc.update(prev)
    btc.update(normalized_market_context(btc, hist))
    btc.update(interaction_scores(btc))
    btc.update(signal_agreement(btc))
    btc.update(setup_readiness(btc))
    btc.update(early_setup_detector(btc))
    btc.update(alert_priority(btc))
    btc.update(bias_confirmation(btc))
    btc.update(direction_consensus(btc))
    btc.update(signal_conflict_detector(btc))
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

    btc.update(trade_lifecycle_state(btc))
    btc.update(setup_classifier(btc))
    btc.update(state_machine_v2(btc))
    btc.update(directional_entry_zones(btc))
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
    btc.update(_derive_canonical(btc))
    btc.update(trade_plan_generator(btc))
    btc.update(build_trade_report(btc))

    out['btc'] = btc
    out['global_data_available'] = True
    out['global_data_error'] = None
    try:
        out['global'] = global_data()
        if out['global'].get('source_global_status') != 'ok':
            out['global_data_available'] = False
            out['global_data_error'] = 'global_data_unavailable'
    except Exception as e:
        out['global_data_available'] = False
        out['global_data_error'] = str(e)
        out['global'] = {
            'total_mcap_usd': None,
            'btc_dom_pct': None,
            'source_global_status': 'exception',
        }
    out.update(data_quality(out))

    metrics = compare_to_previous(out, hist)
    out.update(metrics)
    out['btc'].update(metrics)
    out['btc'].update(state_change_alerts(out['btc']))
    append_history(out)
    return out
