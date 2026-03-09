def liquidity_map_proxy(d):
    above=[]; below=[]; last=d.get('last')
    for key in ['prev_5m_high','prev_15m_high','swing_high_12x5m','vwap_24h']:
        v=d.get(key)
        if v is not None and last is not None and v>last: above.append((abs(v-last),v))
    for key in ['prev_5m_low','prev_15m_low','swing_low_12x5m','vwap_24h','vwap_1h']:
        v=d.get(key)
        if v is not None and last is not None and v<last: below.append((abs(v-last),v))
    above.sort(key=lambda x:x[0]); below.sort(key=lambda x:x[0])
    liq_above_1=above[0][1] if len(above)>0 else None; liq_above_2=above[1][1] if len(above)>1 else None; liq_below_1=below[0][1] if len(below)>0 else None; liq_below_2=below[1][1] if len(below)>1 else None
    nearest='neutral'
    if last is not None and liq_above_1 is not None and liq_below_1 is not None: nearest='above' if abs(liq_above_1-last)<abs(last-liq_below_1) else 'below'
    return {'liq_above_1':liq_above_1,'liq_above_2':liq_above_2,'liq_below_1':liq_below_1,'liq_below_2':liq_below_2,'nearest_liquidity_side':nearest}

def orderbook_wall_tracker(d):
    mid=d.get('orderbook_mid'); bid_wall=d.get('liq_below_1'); ask_wall=d.get('liq_above_1'); ob=d.get('orderbook_imbalance_0_25_pct'); pressure='neutral'
    if ob is not None:
        if ob>8: pressure='bid'
        elif ob<-8: pressure='ask'
    return {'largest_bid_wall_price':bid_wall,'largest_ask_wall_price':ask_wall,'largest_bid_wall_usd':abs(mid-bid_wall)*1000 if None not in (mid,bid_wall) else None,'largest_ask_wall_usd':abs(ask_wall-mid)*1000 if None not in (mid,ask_wall) else None,'wall_pressure_side':pressure}

def trigger_engine(d):
    last=d.get('last'); bull_trigger=d.get('liq_above_1') or d.get('prev_15m_high') or d.get('prev_5m_high'); bear_trigger=d.get('liq_below_1') or d.get('prev_15m_low') or d.get('prev_5m_low'); swing_high=d.get('swing_high_12x5m'); swing_low=d.get('swing_low_12x5m'); atr5=d.get('atr_5m'); atr15=d.get('atr_15m')
    return {'bull_trigger_price':bull_trigger,'bear_trigger_price':bear_trigger,'invalidation_long':swing_low,'invalidation_short':swing_high,'atr_stop_long':last-atr5*1.2 if None not in (last,atr5) else None,'atr_stop_short':last+atr5*1.2 if None not in (last,atr5) else None,'target_long_1':bull_trigger+atr15*0.8 if None not in (bull_trigger,atr15) else None,'target_long_2':bull_trigger+atr15*1.8 if None not in (bull_trigger,atr15) else None,'target_short_1':bear_trigger-atr15*0.8 if None not in (bear_trigger,atr15) else None,'target_short_2':bear_trigger-atr15*1.8 if None not in (bear_trigger,atr15) else None}

def trigger_acceptance(d):
    last=d.get('last'); long_trigger=d.get('bull_trigger_price'); short_trigger=d.get('bear_trigger_price'); body5=d.get('cur5m_body_pct_of_range') or 0.0; upper=d.get('cur5m_upper_wick_pct_of_range') or 0.0; lower=d.get('cur5m_lower_wick_pct_of_range') or 0.0
    above=False; below=False
    if None not in (last,long_trigger): above=last>long_trigger and body5>35 and upper<55
    if None not in (last,short_trigger): below=last<short_trigger and body5>35 and lower<55
    return {'above_long_trigger_acceptance':above,'below_short_trigger_acceptance':below}

def retest_detector(d):
    last=d.get('last'); bull_trigger=d.get('bull_trigger_price'); bear_trigger=d.get('bear_trigger_price'); body5=d.get('cur5m_body_pct_of_range') or 0.0; upper=d.get('cur5m_upper_wick_pct_of_range') or 0.0; lower=d.get('cur5m_lower_wick_pct_of_range') or 0.0; delta=d.get('recent_notional_delta_pct') or 0.0; cvd=d.get('cvd_trend_usd') or 0.0; p1=d.get('price_vs_vwap_1h_pct') or 0.0; ob=d.get('orderbook_imbalance_0_25_pct') or 0.0; nearest=d.get('nearest_liquidity_side')
    long_score=short_score=0.0
    if None not in (last,bull_trigger) and last!=0:
        dist=abs(last-bull_trigger)/last*100.0
        if dist<=0.10: long_score += 35
        elif dist<=0.20: long_score += 20
        elif dist<=0.35: long_score += 10
        if p1>-0.05: long_score += 10
        if delta>0: long_score += min(delta*0.18,15)
        if cvd>0: long_score += 10
        if ob>0: long_score += min(ob*0.8,10)
        if nearest=='above': long_score += 8
        if lower<35: long_score += 6
        if body5>45: long_score += 6
    if None not in (last,bear_trigger) and last!=0:
        dist=abs(last-bear_trigger)/last*100.0
        if dist<=0.10: short_score += 35
        elif dist<=0.20: short_score += 20
        elif dist<=0.35: short_score += 10
        if p1<0.05: short_score += 10
        if delta<0: short_score += min(abs(delta)*0.18,15)
        if cvd<0: short_score += 10
        if ob<0: short_score += min(abs(ob)*0.8,10)
        if nearest=='below': short_score += 8
        if upper<35: short_score += 6
        if body5>45: short_score += 6
    long_score=round(min(max(long_score,0),100),2); short_score=round(min(max(short_score,0),100),2); winner='none'; long_ready=short_ready=False; margin=abs(long_score-short_score)
    if long_score>=45 and long_score>=short_score+8: winner='long'; long_ready=True
    elif short_score>=45 and short_score>=long_score+8: winner='short'; short_ready=True
    return {'retest_long_score':long_score,'retest_short_score':short_score,'retest_winner_side':winner,'retest_long_ready':long_ready,'retest_short_ready':short_ready,'retest_quality_score':max(long_score,short_score),'retest_score_margin':round(margin,2)}
