
from btc_pro_config import clamp, NOISE_FLOOR, REGIME_WEIGHT_PROFILE
from btc_pro_history import rolling_mean, rolling_median, rolling_slope, percentile_rank, robust_zscore

def apply_noise_floor(value, floor):
    if value is None:
        return None
    return 0.0 if abs(value) < floor else value

def capped_log_score(x, low=0.8, high=2.0, max_score=100):
    if x is None:
        return None
    if x <= low:
        return 0.0
    if x >= high:
        return float(max_score)
    span = high - low
    ratio = (x - low) / span
    return round((ratio ** 0.7) * max_score, 2)

def bucket_score(x, buckets):
    # buckets: [(threshold, score), ...] ascending threshold
    if x is None:
        return None
    out = 0.0
    for thr, score in buckets:
        if x >= thr:
            out = score
    return out

def normalized_market_context(btc, history_rows):
    out = {}

    # robust z / percentile ranks
    targets = [
        ("recent_notional_delta_pct", "delta"),
        ("oi_change_15m_pct", "oi15"),
        ("volume_spike_5m_x", "vol5"),
        ("volume_spike_15m_x", "vol15"),
        ("breakout_quality_score", "boq"),
        ("setup_readiness_score", "setup"),
        ("entry_timing_score", "entry"),
        ("orderflow_consistency_score", "orderflow"),
        ("execution_feasibility_score", "exec"),
    ]
    for src_key, prefix in targets:
        v = btc.get(src_key)
        out[f"{prefix}_pct_rank_10"] = percentile_rank(history_rows, src_key, v, 10)
        out[f"{prefix}_robust_z_10"] = robust_zscore(history_rows, src_key, v, 10)

    # noise-floor filtered values
    out["delta_pct_nf"] = apply_noise_floor(btc.get("recent_notional_delta_pct"), NOISE_FLOOR["delta_pct"])
    out["oi_15m_pct_nf"] = apply_noise_floor(btc.get("oi_change_15m_pct"), NOISE_FLOOR["oi_pct"])
    out["spot_perp_div_nf"] = apply_noise_floor(btc.get("spot_perp_divergence_5m_pct_pt"), NOISE_FLOOR["spot_perp_div_pct_pt"])
    out["ob_imbalance_nf"] = apply_noise_floor(btc.get("orderbook_imbalance_0_25_pct"), NOISE_FLOOR["orderbook_imbalance_pct"])
    out["price_vs_vwap_1h_nf"] = apply_noise_floor(btc.get("price_vs_vwap_1h_pct"), NOISE_FLOOR["price_vs_vwap_pct"])

    # rolling slopes
    slope_keys = {
        "bull_score_slope_3": ("bull_score", 3),
        "bear_score_slope_3": ("bear_score", 3),
        "confidence_slope_3": ("confidence_score", 3),
        "volume_quality_slope_3": ("volume_quality_score", 3),
        "breakout_quality_slope_3": ("breakout_quality_score", 3),
        "entry_timing_slope_3": ("entry_timing_score", 3),
        "setup_readiness_slope_3": ("setup_readiness_score", 3),
    }
    for out_key, (src_key, n) in slope_keys.items():
        out[out_key] = rolling_slope(history_rows, src_key, n)

    # rolling means/medians
    for src in ["bull_score", "bear_score", "confidence_score", "volume_quality_score", "breakout_quality_score"]:
        out[f"{src}_mean_5"] = rolling_mean(history_rows, src, 5)
        out[f"{src}_median_5"] = rolling_median(history_rows, src, 5)

    return out

def interaction_scores(btc):
    delta = btc.get("delta_pct_nf") or 0.0
    cvd = btc.get("cvd_trend_usd") or 0.0
    vol5 = btc.get("volume_spike_5m_x") or 0.0
    vol15 = btc.get("volume_spike_15m_x") or 0.0
    oi15 = btc.get("oi_15m_pct_nf") or 0.0
    near = btc.get("nearest_liquidity_side")
    long_trig = btc.get("bull_trigger_price")
    short_trig = btc.get("bear_trigger_price")
    last = btc.get("last")
    body = btc.get("cur5m_body_pct_of_range") or 0.0

    flow_align = 0.0
    if delta > 0 and cvd > 0:
        flow_align += 35
    elif delta < 0 and cvd < 0:
        flow_align += 35
    if vol5 > 0.8 and vol15 > 0.8:
        flow_align += 20
    if oi15 > 0.10:
        flow_align += 10
    if body > 50:
        flow_align += 10

    breakout_cluster = 0.0
    if delta > 5 and vol15 > 0.8 and body > 50:
        breakout_cluster += 35
    if delta < -5 and vol15 > 0.8 and body > 50:
        breakout_cluster += 35
    if btc.get("breakout_direction") in ("up", "down"):
        breakout_cluster += 10
    if btc.get("range_expansion_5m_x") and btc["range_expansion_5m_x"] > 0.9:
        breakout_cluster += 12

    retest_cluster = 0.0
    if last is not None and long_trig is not None and abs(last - long_trig) / last * 100 <= 0.15:
        retest_cluster += 20
    if last is not None and short_trig is not None and abs(last - short_trig) / last * 100 <= 0.15:
        retest_cluster += 20
    if near in ("above", "below"):
        retest_cluster += 10
    if body > 35:
        retest_cluster += 8
    if (btc.get("retest_score_margin") or 0) > 10:
        retest_cluster += 18

    return {
        "flow_alignment_score": round(clamp(flow_align, 0, 100), 2),
        "breakout_confirmation_cluster": round(clamp(breakout_cluster, 0, 100), 2),
        "retest_support_cluster": round(clamp(retest_cluster, 0, 100), 2),
    }

def regime_adaptive_weights(regime):
    return REGIME_WEIGHT_PROFILE.get(regime or "neutral", REGIME_WEIGHT_PROFILE["neutral"])
