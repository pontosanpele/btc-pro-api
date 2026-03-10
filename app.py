from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from pathlib import Path
import json
from typing import Any

from flask import Flask, jsonify, make_response, request

app = Flask(__name__)
SNAPSHOT_FILE = Path("snapshot.json")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_snapshot() -> dict[str, Any]:
    if not SNAPSHOT_FILE.exists():
        raise FileNotFoundError("snapshot not uploaded yet")
    with SNAPSHOT_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_snapshot(data: dict[str, Any]) -> None:
    data["server_updated_at"] = utc_now_iso()
    with SNAPSHOT_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _apply_no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.after_request
def add_no_cache_headers(resp):
    return _apply_no_cache(resp)


def _json_response(payload: Any, status: int = 200):
    return _apply_no_cache(make_response(jsonify(payload), status))


def _text_response(text: str, status: int = 200, content_type: str = "text/plain; charset=utf-8"):
    resp = make_response(text, status)
    resp.headers["Content-Type"] = content_type
    return _apply_no_cache(resp)


def _pretty_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _fmt_num(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, bool):
        return "igen" if v else "nem"
    if isinstance(v, (int, float)):
        if abs(v) >= 1000:
            return f"{v:,.2f}".replace(",", " ")
        return f"{v:.2f}"
    return str(v)


def _fmt_zone(v: Any) -> str:
    if v is None or v == "":
        return "-"
    if isinstance(v, (list, tuple)):
        if len(v) == 2:
            return f"{_fmt_num(v[0])} – {_fmt_num(v[1])}"
        return ", ".join(_fmt_num(x) for x in v)
    return _fmt_num(v)


def _pick_entry_zone(side: str, btc: dict[str, Any], report: dict[str, Any]) -> Any:
    direct = report.get(f"{side}_entry_zone") or btc.get(f"{side}_entry_zone")
    if direct not in (None, "", []):
        return direct

    trade_side = btc.get("trade_plan_side")
    trade_zone = btc.get("trade_plan_entry_zone")
    if trade_side == side and trade_zone not in (None, "", []):
        return trade_zone

    trigger = btc.get("bull_trigger_price") if side == "long" else btc.get("bear_trigger_price")
    atr = btc.get("atr_5m") or 0
    if trigger is not None and isinstance(atr, (int, float)) and atr > 0:
        half = max(atr * 0.08, trigger * 0.00035)
        return [round(trigger - half, 2), round(trigger + half, 2)]
    return None


def _extract_trade_view(data: dict[str, Any]) -> dict[str, Any]:
    btc = data.get("btc", {})
    report = btc.get("trade_report", {})
    long_entry = _pick_entry_zone("long", btc, report)
    short_entry = _pick_entry_zone("short", btc, report)
    return {
        "ts_bucharest": data.get("ts_bucharest"),
        "server_updated_at": data.get("server_updated_at"),
        "price": btc.get("last"),
        "direction": report.get("direction") or btc.get("final_side") or btc.get("trade_bias"),
        "verdict": report.get("verdict") or btc.get("summary_status") or btc.get("final_action"),
        "market_regime": btc.get("market_regime"),
        "dominant_bias_htf": btc.get("dominant_bias_htf"),
        "execution_bias_ltf": btc.get("execution_bias_ltf"),
        "long": {
            "entry_zone": long_entry,
            "entry_zone_text": _fmt_zone(long_entry),
            "sl": report.get("long_sl") or btc.get("atr_stop_long") or btc.get("invalidation_long"),
            "tp1": report.get("long_tp1") or btc.get("target_long_1"),
            "tp2": report.get("long_tp2") or btc.get("target_long_2"),
        },
        "short": {
            "entry_zone": short_entry,
            "entry_zone_text": _fmt_zone(short_entry),
            "sl": report.get("short_sl") or btc.get("atr_stop_short") or btc.get("invalidation_short"),
            "tp1": report.get("short_tp1") or btc.get("target_short_1"),
            "tp2": report.get("short_tp2") or btc.get("target_short_2"),
        },
        "key_levels": report.get("key_levels") or {
            "bull_trigger": btc.get("bull_trigger_price"),
            "bear_trigger": btc.get("bear_trigger_price"),
            "liq_above_1": btc.get("liq_above_1"),
            "liq_below_1": btc.get("liq_below_1"),
        },
    }


def _extract_next15(data: dict[str, Any]) -> dict[str, Any]:
    btc = data.get("btc", {})
    direction = btc.get("execution_bias_ltf") or btc.get("final_side") or btc.get("trade_bias") or "neutral"
    reasons = []
    for k, ok in [
        ("retest_long_ready", btc.get("retest_long_ready")),
        ("retest_short_ready", btc.get("retest_short_ready")),
        ("breakout_direction", btc.get("breakout_direction")),
        ("dominant_bias_htf", btc.get("dominant_bias_htf")),
        ("market_regime", btc.get("market_regime")),
        ("wall_pressure_side", btc.get("wall_pressure_side")),
    ]:
        if ok not in (None, False, "", "neutral"):
            reasons.append(f"{k}: {ok}")
    return {
        "ts_bucharest": data.get("ts_bucharest"),
        "server_updated_at": data.get("server_updated_at"),
        "next_15m_bias": direction,
        "confidence": btc.get("confidence_score") or btc.get("trade_plan_confidence"),
        "verdict": btc.get("summary_status") or btc.get("final_action") or btc.get("trade_bias"),
        "bull_trigger": btc.get("bull_trigger_price"),
        "bear_trigger": btc.get("bear_trigger_price"),
        "long_entry_zone": _fmt_zone(btc.get("long_entry_zone")),
        "short_entry_zone": _fmt_zone(btc.get("short_entry_zone")),
        "reasons": reasons,
    }


def _nav() -> str:
    ts = escape(request.args.get("ts", "1"))
    return (
        "<nav class='nav'>"
        f"<a href='/?ts={ts}'>Kezdőlap</a>"
        f"<a href='/snapshot-view?ts={ts}'>Snapshot</a>"
        f"<a href='/trade-view?ts={ts}'>Trade</a>"
        f"<a href='/next15-view?ts={ts}'>Köv. 15p</a>"
        "</nav>"
    )


def _layout(title: str, body: str) -> str:
    bust = escape(request.args.get("ts", "-"))
    return f"""<!doctype html>
<html lang='hu'>
<head>
<meta charset='utf-8'>
<meta http-equiv='Cache-Control' content='no-store, no-cache, must-revalidate, max-age=0'>
<meta http-equiv='Pragma' content='no-cache'>
<meta http-equiv='Expires' content='0'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>{escape(title)}</title>
<style>
:root{{--bg:#0f172a;--card:#111827;--card2:#1f2937;--txt:#e5e7eb;--muted:#9ca3af;--green:#16a34a;--red:#dc2626;--blue:#2563eb;}}
*{{box-sizing:border-box}} body{{margin:0;background:linear-gradient(180deg,#0b1220,#111827);color:var(--txt);font-family:Inter,system-ui,Arial,sans-serif}}
.wrap{{max-width:1100px;margin:0 auto;padding:20px}} .nav{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:18px}}
.nav a{{background:#1d4ed8;color:white;text-decoration:none;padding:10px 14px;border-radius:12px;font-weight:600}}
.hero,.card{{background:rgba(17,24,39,.95);border:1px solid #2a3345;border-radius:18px;box-shadow:0 10px 30px rgba(0,0,0,.25)}}
.hero{{padding:20px;margin-bottom:18px}} .muted{{color:var(--muted)}} .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}}
.card{{padding:18px}} .title{{font-size:20px;font-weight:700;margin:0 0 8px}} .pill{{display:inline-block;padding:6px 10px;border-radius:999px;background:#243044;font-size:12px;font-weight:700}}
.table{{width:100%;border-collapse:collapse}} .table td{{padding:8px 0;border-bottom:1px solid #253044}} .table td:last-child{{text-align:right;font-weight:700}}
.long{{border-left:5px solid var(--green)}} .short{{border-left:5px solid var(--red)}} pre{{white-space:pre-wrap;word-break:break-word;background:#0b1220;padding:14px;border-radius:12px}}
.small{{font-size:12px;color:var(--muted)}}
</style>
</head><body><div class='wrap'>{_nav()}<div class='hero'><div class='title'>{escape(title)}</div><div class='small'>cache-bust ts: {bust}</div></div>{body}</div></body></html>"""


def _trade_body(payload: dict[str, Any]) -> str:
    long = payload["long"]
    short = payload["short"]
    levels = payload.get("key_levels", {})
    body = f"""
<div class='grid'>
  <div class='card'>
    <div class='title'>Piaci összkép</div>
    <table class='table'>
      <tr><td>Idő</td><td>{escape(str(payload.get('ts_bucharest', '-')))}</td></tr>
      <tr><td>Ár</td><td>{_fmt_num(payload.get('price'))}</td></tr>
      <tr><td>Irány</td><td>{escape(str(payload.get('direction', '-')))}</td></tr>
      <tr><td>Verdikt</td><td>{escape(str(payload.get('verdict', '-')))}</td></tr>
      <tr><td>Rezsim</td><td>{escape(str(payload.get('market_regime', '-')))}</td></tr>
      <tr><td>HTF bias</td><td>{escape(str(payload.get('dominant_bias_htf', '-')))}</td></tr>
      <tr><td>LTF bias</td><td>{escape(str(payload.get('execution_bias_ltf', '-')))}</td></tr>
    </table>
  </div>
  <div class='card long'>
    <div class='title'>Long lehetőség</div>
    <table class='table'>
      <tr><td>Belépő zóna</td><td>{escape(long.get('entry_zone_text', '-'))}</td></tr>
      <tr><td>SL</td><td>{_fmt_num(long.get('sl'))}</td></tr>
      <tr><td>TP1</td><td>{_fmt_num(long.get('tp1'))}</td></tr>
      <tr><td>TP2</td><td>{_fmt_num(long.get('tp2'))}</td></tr>
    </table>
  </div>
  <div class='card short'>
    <div class='title'>Short lehetőség</div>
    <table class='table'>
      <tr><td>Belépő zóna</td><td>{escape(short.get('entry_zone_text', '-'))}</td></tr>
      <tr><td>SL</td><td>{_fmt_num(short.get('sl'))}</td></tr>
      <tr><td>TP1</td><td>{_fmt_num(short.get('tp1'))}</td></tr>
      <tr><td>TP2</td><td>{_fmt_num(short.get('tp2'))}</td></tr>
    </table>
  </div>
  <div class='card'>
    <div class='title'>Kulcsszintek</div>
    <table class='table'>
      {''.join(f'<tr><td>{escape(str(k))}</td><td>{escape(_fmt_zone(v) if isinstance(v,(list,tuple)) else _fmt_num(v))}</td></tr>' for k,v in levels.items())}
    </table>
  </div>
</div>"""
    return body


def _snapshot_body(payload: Any) -> str:
    return f"<div class='card'><pre>{escape(_pretty_json(payload))}</pre></div>"


def _next15_body(payload: dict[str, Any]) -> str:
    items = ''.join(f"<li>{escape(str(x))}</li>" for x in payload.get('reasons', [])) or '<li>-</li>'
    return f"""
<div class='grid'>
  <div class='card'>
    <div class='title'>Következő 15 perc</div>
    <table class='table'>
      <tr><td>Idő</td><td>{escape(str(payload.get('ts_bucharest', '-')))}</td></tr>
      <tr><td>Bias</td><td>{escape(str(payload.get('next_15m_bias', '-')))}</td></tr>
      <tr><td>Bizalom</td><td>{_fmt_num(payload.get('confidence'))}</td></tr>
      <tr><td>Verdikt</td><td>{escape(str(payload.get('verdict', '-')))}</td></tr>
      <tr><td>Long zóna</td><td>{escape(str(payload.get('long_entry_zone', '-')))}</td></tr>
      <tr><td>Short zóna</td><td>{escape(str(payload.get('short_entry_zone', '-')))}</td></tr>
      <tr><td>Bull trigger</td><td>{_fmt_num(payload.get('bull_trigger'))}</td></tr>
      <tr><td>Bear trigger</td><td>{_fmt_num(payload.get('bear_trigger'))}</td></tr>
    </table>
  </div>
  <div class='card'><div class='title'>Indokok</div><ul>{items}</ul></div>
</div>"""


@app.route('/')
def home():
    body = """
<div class='grid'>
  <div class='card'><div class='title'>Snapshot</div><p class='muted'>Teljes nyers adat és szép nézet.</p></div>
  <div class='card'><div class='title'>Trade</div><p class='muted'>Emberbarát long / short riport belépő zónával.</p></div>
  <div class='card'><div class='title'>Köv. 15 perc</div><p class='muted'>Gyors bias és trigger nézet.</p></div>
</div>"""
    return _text_response(_layout('BTC Snapshot Dashboard', body), content_type='text/html; charset=utf-8')


@app.route('/snapshot', methods=['GET'])
def get_snapshot():
    try:
        return _json_response(_load_snapshot())
    except FileNotFoundError as e:
        return _json_response({"ok": False, "error": str(e)}, 404)
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)}, 500)


@app.route('/snapshot-pretty', methods=['GET'])
def get_snapshot_pretty():
    try:
        return _text_response(_pretty_json(_load_snapshot()))
    except FileNotFoundError as e:
        return _text_response(_pretty_json({"ok": False, "error": str(e)}), 404)
    except Exception as e:
        return _text_response(_pretty_json({"ok": False, "error": str(e)}), 500)


@app.route('/snapshot-view', methods=['GET'])
def get_snapshot_view():
    try:
        return _text_response(_layout('Snapshot View', _snapshot_body(_load_snapshot())), content_type='text/html; charset=utf-8')
    except FileNotFoundError as e:
        return _text_response(_layout('Snapshot View', _snapshot_body({"ok": False, "error": str(e)})), 404, 'text/html; charset=utf-8')
    except Exception as e:
        return _text_response(_layout('Snapshot View', _snapshot_body({"ok": False, "error": str(e)})), 500, 'text/html; charset=utf-8')


@app.route('/trade', methods=['GET'])
@app.route('/next15', methods=['GET'])
def get_trade_json():
    try:
        data = _load_snapshot()
        return _json_response(_extract_next15(data) if request.path.endswith('next15') else _extract_trade_view(data))
    except FileNotFoundError as e:
        return _json_response({"ok": False, "error": str(e)}, 404)
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)}, 500)


@app.route('/trade-pretty', methods=['GET'])
@app.route('/next15-pretty', methods=['GET'])
def get_trade_pretty():
    try:
        data = _load_snapshot()
        payload = _extract_next15(data) if request.path.endswith('next15-pretty') else _extract_trade_view(data)
        return _text_response(_pretty_json(payload))
    except FileNotFoundError as e:
        return _text_response(_pretty_json({"ok": False, "error": str(e)}), 404)
    except Exception as e:
        return _text_response(_pretty_json({"ok": False, "error": str(e)}), 500)


@app.route('/trade-view', methods=['GET'])
@app.route('/next15-view', methods=['GET'])
def get_trade_view_html():
    try:
        data = _load_snapshot()
        if request.path.endswith('next15-view'):
            payload = _extract_next15(data)
            return _text_response(_layout('Következő 15 perc', _next15_body(payload)), content_type='text/html; charset=utf-8')
        payload = _extract_trade_view(data)
        return _text_response(_layout('Trade jelentés', _trade_body(payload)), content_type='text/html; charset=utf-8')
    except FileNotFoundError as e:
        return _text_response(_layout('Trade View', _snapshot_body({"ok": False, "error": str(e)})), 404, 'text/html; charset=utf-8')
    except Exception as e:
        return _text_response(_layout('Trade View', _snapshot_body({"ok": False, "error": str(e)})), 500, 'text/html; charset=utf-8')


@app.route('/upload', methods=['POST'])
def upload_snapshot():
    try:
        data = request.get_json(force=True)
        if not isinstance(data, dict):
            return _json_response({"ok": False, "error": "JSON object expected"}, 400)
        _save_snapshot(data)
        return _json_response({"ok": True, "server_updated_at": data.get("server_updated_at")})
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)}, 500)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
