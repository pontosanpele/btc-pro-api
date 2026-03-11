from flask import Flask, jsonify, request, Response
from pathlib import Path
import json, html
from datetime import datetime, timezone

app = Flask(__name__)
SNAPSHOT_FILE = Path("snapshot.json")

def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def _no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

@app.after_request
def _after(resp):
    return _no_cache(resp)

def _load_snapshot():
    if not SNAPSHOT_FILE.exists():
        raise FileNotFoundError('snapshot not uploaded yet')
    return json.loads(SNAPSHOT_FILE.read_text(encoding='utf-8'))

def _save_snapshot(data):
    data['server_updated_at'] = _utc_now_iso()
    SNAPSHOT_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

def _r(v, n=2):
    try: return round(float(v), n)
    except Exception: return None

def _get(d, *keys, default=None):
    cur=d
    for k in keys:
        if not isinstance(cur, dict): return default
        cur=cur.get(k)
        if cur is None: return default
    return cur

def _zone(z):
    if isinstance(z, (list, tuple)) and len(z)>=2 and z[0] is not None and z[1] is not None:
        return [_r(z[0]), _r(z[1])]
    return None

def _extract_trade_view(data):
    b=data.get('btc', {})
    tr=b.get('trade_report', {})
    long_zone=_zone(tr.get('long_entry_zone')) or _zone(b.get('long_entry_zone')) or _zone(b.get('trade_plan_entry_zone') if b.get('trade_plan_side')=='long' else None)
    short_zone=_zone(tr.get('short_entry_zone')) or _zone(b.get('short_entry_zone')) or _zone(b.get('trade_plan_entry_zone') if b.get('trade_plan_side')=='short' else None)
    return {
        'ts_bucharest': data.get('ts_bucharest'),
        'server_updated_at': data.get('server_updated_at'),
        'price': _r(b.get('last')),
        'htf_bias': b.get('dominant_bias_htf','-'),
        'htf_context': b.get('dominant_bias_context','-'),
        'ltf_bias': b.get('execution_bias_ltf','-'),
        'ltf_context': b.get('execution_bias_context','-'),
        'analysis_bias': b.get('analysis_bias','-'),
        'execution_status': b.get('execution_status','-'),
        'trade_plan_status': b.get('trade_plan_status','-'),
        'verdict': b.get('canonical_final_action') or b.get('final_action_v4') or b.get('final_action_v3') or b.get('final_action_v2') or b.get('final_action') or tr.get('verdict','WAIT'),
        'confidence': _r(b.get('trade_plan_confidence') or b.get('confidence_score')),
        'long_entry_zone': long_zone,
        'long_entry_zone_aggressive': _zone(tr.get('long_entry_zone_aggressive')) or _zone(b.get('long_entry_zone_aggressive')),
        'long_entry_zone_conservative': _zone(tr.get('long_entry_zone_conservative')) or _zone(b.get('long_entry_zone_conservative')),
        'long_countertrend': bool(tr.get('long_countertrend') or b.get('long_is_countertrend')),
        'long_sl': _r(tr.get('long_sl') or b.get('atr_stop_long')),
        'long_tp1': _r(tr.get('long_tp1') or b.get('target_long_1')),
        'long_tp2': _r(tr.get('long_tp2') or b.get('target_long_2')),
        'short_entry_zone': short_zone,
        'short_entry_zone_aggressive': _zone(tr.get('short_entry_zone_aggressive')) or _zone(b.get('short_entry_zone_aggressive')),
        'short_entry_zone_conservative': _zone(tr.get('short_entry_zone_conservative')) or _zone(b.get('short_entry_zone_conservative')),
        'short_countertrend': bool(tr.get('short_countertrend') or b.get('short_is_countertrend')),
        'short_sl': _r(tr.get('short_sl') or b.get('atr_stop_short')),
        'short_tp1': _r(tr.get('short_tp1') or b.get('target_short_1')),
        'short_tp2': _r(tr.get('short_tp2') or b.get('target_short_2')),
        'bull_trigger': _r(b.get('bull_trigger_price')),
        'bear_trigger': _r(b.get('bear_trigger_price')),
        'reasons': (b.get('canonical_final_reason') or b.get('final_reason_v4') or b.get('final_reason_v3') or b.get('final_reason_v2') or b.get('final_reason') or []),
    }

def _extract_next15(data):
    p=_extract_trade_view(data); b=data.get('btc',{})
    p.update({
        'next_15m_bias': b.get('trade_bias','neutral'),
        'retest_winner_side': b.get('retest_winner_side','-'),
        'market_regime': b.get('market_regime','-'),
        'confidence_direction': _r(b.get('confidence_direction')),
        'confidence_execution': _r(b.get('confidence_execution')),
        'confidence_rr': _r(b.get('confidence_rr')),
        'confidence_external': _r(b.get('confidence_external')),
    })
    return p

def _pretty(obj):
    return json.dumps(obj, ensure_ascii=False, indent=2)

def _resp_text(txt, status=200, ctype='text/plain; charset=utf-8'):
    return Response(txt, status=status, mimetype=ctype)

def _json(obj, status=200):
    r=jsonify(obj); r.status_code=status; return r

def _layout(title, body):
    nav="<nav><a href='/'>Főoldal</a><a href='/snapshot-view'>Snapshot</a><a href='/trade-view'>Trade</a><a href='/next15-view'>Köv. 15p</a></nav>"
    css="body{font-family:Inter,Arial,sans-serif;margin:0;background:#0b1220;color:#e5e7eb}nav{display:flex;gap:14px;align-items:center;padding:14px 18px;background:#111827;border-bottom:1px solid #1f2937;position:sticky;top:0}nav a{color:#cbd5e1;text-decoration:none;padding:8px 10px;border-radius:10px;background:#0f172a}nav a:hover{background:#172033}.wrap{max-width:1180px;margin:24px auto;padding:0 16px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px}.card{background:#111827;border:1px solid #1f2937;border-radius:18px;padding:16px}.title{font-weight:700;margin-bottom:10px}.muted{color:#94a3b8}.mono{font-family:ui-monospace,Menlo,monospace}table{width:100%;border-collapse:collapse}td{padding:8px 0;border-bottom:1px solid #1f2937;vertical-align:top}.good{color:#86efac}.bad{color:#fca5a5}.pill{display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:700}.pill-long{background:#052e16;color:#86efac;border:1px solid #166534}.pill-short{background:#3f0a0a;color:#fca5a5;border:1px solid #7f1d1d}.pill-neutral{background:#1f2937;color:#cbd5e1;border:1px solid #334155}.section-long{border-left:4px solid #16a34a}.section-short{border-left:4px solid #dc2626}.pre{white-space:pre-wrap;word-break:break-word}"
    return f"<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>{html.escape(title)}</title><style>{css}</style></head><body>{nav}<div class='wrap'><h1>{html.escape(title)}</h1>{body}</div></body></html>"

def _zone_txt(z):
    z=_zone(z)
    return '-' if not z else f"{z[0]} – {z[1]}"

def _pill(value):
    v=str(value or '-').lower()
    cls='pill-neutral'
    if 'long' in v: cls='pill-long'
    elif 'short' in v: cls='pill-short'
    return f"<span class='pill {cls}'>{html.escape(str(value or '-'))}</span>"


def _trade_body(p):
    reasons=''.join(f"<li>{html.escape(str(x))}</li>" for x in (p.get('reasons') or ['-']))
    long_extra = f"<tr><td>Aggresszív</td><td>{_zone_txt(p.get('long_entry_zone_aggressive'))}</td></tr><tr><td>Konzervatív</td><td>{_zone_txt(p.get('long_entry_zone_conservative'))}</td></tr>"
    short_extra = f"<tr><td>Aggresszív</td><td>{_zone_txt(p.get('short_entry_zone_aggressive'))}</td></tr><tr><td>Konzervatív</td><td>{_zone_txt(p.get('short_entry_zone_conservative'))}</td></tr>"
    return f"<div class='grid'><div class='card'><div class='title'>Bias összkép</div><table><tr><td>HTF bias</td><td>{_pill(p.get('htf_bias'))}</td></tr><tr><td>HTF context</td><td>{html.escape(str(p.get('htf_context')))}</td></tr><tr><td>LTF bias</td><td>{_pill(p.get('ltf_bias'))}</td></tr><tr><td>LTF context</td><td>{html.escape(str(p.get('ltf_context')))}</td></tr><tr><td>Analysis bias</td><td>{_pill(p.get('analysis_bias'))}</td></tr><tr><td>Execution status</td><td>{html.escape(str(p.get('execution_status')))}</td></tr><tr><td>Trade plan</td><td>{html.escape(str(p.get('trade_plan_status')))}</td></tr><tr><td>Verdict</td><td>{_pill(p.get('verdict'))}</td></tr><tr><td>Bizalom</td><td>{p.get('confidence')}</td></tr><tr><td>Ár</td><td>{p.get('price')}</td></tr></table></div><div class='card section-long'><div class='title good'>Long lehetőség</div><table><tr><td>Belépő zóna</td><td>{_zone_txt(p.get('long_entry_zone'))}</td></tr>{long_extra}<tr><td>Countertrend</td><td>{'igen' if p.get('long_countertrend') else 'nem'}</td></tr><tr><td>SL</td><td>{p.get('long_sl')}</td></tr><tr><td>TP1</td><td>{p.get('long_tp1')}</td></tr><tr><td>TP2</td><td>{p.get('long_tp2')}</td></tr></table></div><div class='card section-short'><div class='title bad'>Short lehetőség</div><table><tr><td>Belépő zóna</td><td>{_zone_txt(p.get('short_entry_zone'))}</td></tr>{short_extra}<tr><td>Countertrend</td><td>{'igen' if p.get('short_countertrend') else 'nem'}</td></tr><tr><td>SL</td><td>{p.get('short_sl')}</td></tr><tr><td>TP1</td><td>{p.get('short_tp1')}</td></tr><tr><td>TP2</td><td>{p.get('short_tp2')}</td></tr></table></div><div class='card'><div class='title'>Kulcsszintek</div><table><tr><td>Bull trigger</td><td>{p.get('bull_trigger')}</td></tr><tr><td>Bear trigger</td><td>{p.get('bear_trigger')}</td></tr><tr><td>Idő</td><td>{html.escape(str(p.get('ts_bucharest')))}</td></tr></table></div><div class='card'><div class='title'>Indokok</div><ul>{reasons}</ul></div></div>"

@app.route('/')

def home():
    body="<div class='grid'><div class='card'><div class='title'>Snapshot</div><p class='muted'>/snapshot-view</p></div><div class='card'><div class='title'>Trade</div><p class='muted'>/trade-view</p></div><div class='card'><div class='title'>Köv. 15 perc</div><p class='muted'>/next15-view</p></div></div>"
    return _resp_text(_layout('BTC Snapshot Dashboard', body), ctype='text/html; charset=utf-8')

@app.route('/snapshot')
def snapshot_json():
    try: return _json(_load_snapshot())
    except FileNotFoundError as e: return _json({'ok':False,'error':str(e)},404)
    except Exception as e: return _json({'ok':False,'error':str(e)},500)

@app.route('/snapshot-pretty')
def snapshot_pretty():
    try: return _resp_text(_pretty(_load_snapshot()))
    except FileNotFoundError as e: return _resp_text(_pretty({'ok':False,'error':str(e)}),404)
    except Exception as e: return _resp_text(_pretty({'ok':False,'error':str(e)}),500)

@app.route('/snapshot-view')
def snapshot_view():
    try: body=f"<div class='card pre mono'>{html.escape(_pretty(_load_snapshot()))}</div>"; return _resp_text(_layout('Snapshot', body), ctype='text/html; charset=utf-8')
    except FileNotFoundError as e: return _resp_text(_layout('Snapshot', f"<div class='card'>{html.escape(str(e))}</div>"),404,'text/html; charset=utf-8')
    except Exception as e: return _resp_text(_layout('Snapshot', f"<div class='card'>{html.escape(str(e))}</div>"),500,'text/html; charset=utf-8')

@app.route('/trade')
@app.route('/next15')
def trade_json():
    try:
        data=_load_snapshot(); payload=_extract_next15(data) if request.path.endswith('next15') else _extract_trade_view(data)
        return _json(payload)
    except FileNotFoundError as e: return _json({'ok':False,'error':str(e)},404)
    except Exception as e: return _json({'ok':False,'error':str(e)},500)

@app.route('/trade-pretty')
@app.route('/next15-pretty')
def trade_pretty():
    try:
        data=_load_snapshot(); payload=_extract_next15(data) if request.path.endswith('next15-pretty') else _extract_trade_view(data)
        return _resp_text(_pretty(payload))
    except FileNotFoundError as e: return _resp_text(_pretty({'ok':False,'error':str(e)}),404)
    except Exception as e: return _resp_text(_pretty({'ok':False,'error':str(e)}),500)

@app.route('/trade-view')
@app.route('/next15-view')
def trade_view():
    try:
        data=_load_snapshot(); payload=_extract_next15(data) if request.path.endswith('next15-view') else _extract_trade_view(data)
        body=_next15_body(payload) if request.path.endswith('next15-view') else _trade_body(payload)
        title='Következő 15 perc' if request.path.endswith('next15-view') else 'Trade jelentés'
        return _resp_text(_layout(title, body), ctype='text/html; charset=utf-8')
    except FileNotFoundError as e: return _resp_text(_layout('Trade', f"<div class='card'>{html.escape(str(e))}</div>"),404,'text/html; charset=utf-8')
    except Exception as e: return _resp_text(_layout('Trade', f"<div class='card'>{html.escape(str(e))}</div>"),500,'text/html; charset=utf-8')

@app.route('/upload', methods=['POST'])
def upload():
    try:
        data=request.get_json(force=True)
        if not isinstance(data, dict): return _json({'ok':False,'error':'JSON object expected'},400)
        _save_snapshot(data)
        return _json({'ok':True,'server_updated_at':data.get('server_updated_at')})
    except Exception as e:
        return _json({'ok':False,'error':str(e)},500)

if __name__=='__main__':
    app.run(host='0.0.0.0', port=10000)
