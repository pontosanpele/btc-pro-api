from btc_pro_config import clamp

def chop_filter(d):
    range15=d.get('range_15m'); atr15=d.get('atr_15m')
    return {'chop_score':None if atr15 in (None,0) or range15 is None else range15/atr15}

def multi_tf_trend(d):
    t5=t15=t1h='range'; p1=d.get('price_vs_vwap_1h_pct'); p24=d.get('price_vs_vwap_24h_pct'); delta=d.get('recent_notional_delta_pct'); oi15=d.get('oi_change_15m_pct'); vol15=d.get('volume_spike_15m_x')
    if p1 is not None and delta is not None:
        if p1>0 and delta>5: t5='up'
        elif p1<0 and delta<-5: t5='down'
    if p1 is not None and oi15 is not None and vol15 is not None:
        if p1>0 and vol15>1.0 and oi15<=0.25: t15='up'
        elif p1<0 and vol15>1.0 and oi15>=-0.25: t15='down'
    if p24 is not None:
        if p24>0.15: t1h='up'
        elif p24<-0.15: t1h='down'
    align=0
    for t in (t5,t15,t1h):
        if t=='up': align += 1
        elif t=='down': align -= 1
    return {'trend_5m':t5,'trend_15m':t15,'trend_1h':t1h,'multi_tf_alignment_score':align}

def delta_divergence_detector(d):
    bull_div=bear_div=False; notes=[]
    low2=d.get('price_low_2_5m'); low6=d.get('price_low_6_5m'); high2=d.get('price_high_2_5m'); high6=d.get('price_high_6_5m'); cvd_now=d.get('cvd_last_100_usd'); cvd_trend=d.get('cvd_trend_usd'); vol_declining=d.get('vol_declining_5m')
    if None not in (low2,low6,cvd_now,cvd_trend) and low2<low6 and cvd_now>0 and cvd_trend>0 and vol_declining:
        bull_div=True; notes.append('price_lower_low_cvd_higher_low')
    if None not in (high2,high6,cvd_now,cvd_trend) and high2>high6 and cvd_now<0 and cvd_trend<0 and vol_declining:
        bear_div=True; notes.append('price_higher_high_cvd_lower_high')
    return {'bull_divergence':bull_div,'bear_divergence':bear_div,'divergence_notes':notes}

def absorption_detector(d):
    buy_abs=sell_abs=False; reason=[]
    delta=d.get('recent_notional_delta_pct'); range5=d.get('range_expansion_5m_x'); body5=d.get('cur5m_body_pct_of_range'); lower=d.get('cur5m_lower_wick_pct_of_range'); upper=d.get('cur5m_upper_wick_pct_of_range'); p1=d.get('price_vs_vwap_1h_pct'); vol5=d.get('volume_spike_5m_x')
    if None not in (delta,range5,lower,body5,vol5) and delta<-10 and range5<1.0 and lower>35 and body5<60 and vol5<1.0:
        buy_abs=True; reason.append('sell_flow_absorbed')
    if None not in (delta,range5,upper,body5,vol5) and delta>10 and range5<1.0 and upper>35 and body5<60 and vol5<1.0:
        sell_abs=True; reason.append('buy_flow_absorbed')
    if p1 is not None:
        if buy_abs and p1<0: reason.append('below_vwap_supportive')
        if sell_abs and p1>0: reason.append('above_vwap_resistive')
    return {'absorption_buying':buy_abs,'absorption_selling':sell_abs,'absorption_reason':reason}

def flow_metrics(d):
    delta=d.get('recent_notional_delta_pct') or 0.0; cvd=d.get('cvd_trend_usd') or 0.0; cvd_norm=clamp(cvd/250000.0*100.0,-100,100)
    return {'delta_strength_score':round(clamp(delta*1.2 + cvd_norm*0.6,-100,100),2)}

def volume_quality(d):
    vol5=d.get('volume_spike_5m_x') or 0.0; vol15=d.get('volume_spike_15m_x') or 0.0; body5=d.get('cur5m_body_pct_of_range') or 0.0; trend=10 if (d.get('turnover_5m_usd') or 0)>0 and (d.get('turnover_15m_usd') or 0)>(d.get('turnover_5m_usd') or 0) else 0
    score=min(vol5*35,35)+min(vol15*35,35)+min(body5*0.2,20)+trend
    return {'volume_quality_score':round(clamp(score,0,100),2)}
