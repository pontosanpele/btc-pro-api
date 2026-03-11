from flask import Flask, jsonify, request, Response, render_template_string
from pathlib import Path
from datetime import datetime, timezone
import json
import html

app = Flask(__name__)
SNAPSHOT_FILE = Path("snapshot.json")


def load_snapshot():
    if not SNAPSHOT_FILE.exists():
        return None
    with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def add_server_updated_at(data: dict) -> dict:
    if isinstance(data, dict):
        data["server_updated_at"] = datetime.now(timezone.utc).isoformat()
    return data


@app.after_request
def no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


def _fmt_num(v, nd=2):
    try:
        if v is None:
            return "-"
        return f"{float(v):,.{nd}f}"
    except Exception:
        return str(v)


def _fmt_pct(v, nd=2):
    try:
        if v is None:
            return "-"
        return f"{float(v):.{nd}f}%"
    except Exception:
        return str(v)


def _as_zone(v):
    if isinstance(v, (list, tuple)) and len(v) >= 2 and v[0] is not None and v[1] is not None:
        a, b = float(v[0]), float(v[1])
        lo, hi = (a, b) if a <= b else (b, a)
        return [round(lo, 2), round(hi, 2)]
    return None


def _fallback_zone(btc, side):
    atr5 = float(btc.get("atr_5m") or 0)
    if side == "long":
        trigger = btc.get("bull_trigger_price")
        support = _as_zone([btc.get("support_zone_low"), btc.get("support_zone_high")])
        bid = btc.get("largest_bid_wall_price")
        if support:
            return support
        if trigger is not None:
            lo = max(x for x in [btc.get("invalidation_long"), bid, float(trigger) - max(atr5 * 0.8, 40)] if x is not None)
            hi = float(trigger)
            return [round(min(lo, hi), 2), round(max(lo, hi), 2)]
    else:
        trigger = btc.get("bear_trigger_price")
        resistance = _as_zone([btc.get("resistance_zone_low"), btc.get("resistance_zone_high")])
        ask = btc.get("largest_ask_wall_price")
        if resistance:
            return resistance
        if trigger is not None:
            lo = float(trigger)
            hi = min(x for x in [ask, btc.get("invalidation_short"), float(trigger) + max(atr5 * 1.5, 60)] if x is not None)
            return [round(min(lo, hi), 2), round(max(lo, hi), 2)]
    return None


def _trade_view_model(data):
    btc = (data or {}).get("btc", {})
    trade_report = btc.get("trade_report") or {}
    trade_side = btc.get("trade_plan_side") if btc.get("trade_plan_side") in ("long", "short") else btc.get("trade_bias")

    long_zone = _as_zone(trade_report.get("long_entry_zone")) or _as_zone(btc.get("long_entry_zone"))
    short_zone = _as_zone(trade_report.get("short_entry_zone")) or _as_zone(btc.get("short_entry_zone"))

    tp_zone = _as_zone(btc.get("trade_plan_entry_zone"))
    if trade_side == "long" and not long_zone:
        long_zone = tp_zone
    if trade_side == "short" and not short_zone:
        short_zone = tp_zone

    long_zone = long_zone or _fallback_zone(btc, "long")
    short_zone = short_zone or _fallback_zone(btc, "short")

    return {
        "ts": data.get("ts_bucharest", "-"),
        "server_updated_at": data.get("server_updated_at", "-"),
        "last": btc.get("last"),
        "verdict": trade_report.get("verdict") or str((btc.get("canonical_final_action") or btc.get("final_tier") or btc.get("final_action") or btc.get("trade_bias") or "WAIT")).upper(),
        "trade_side": trade_side or "no_trade",
        "htf_bias": btc.get("dominant_bias_htf", "-"),
        "htf_context": btc.get("dominant_bias_context", "-"),
        "ltf_bias": btc.get("execution_bias_ltf", "-"),
        "ltf_context": btc.get("execution_bias_context", "-"),
        "canonical_action": btc.get("canonical_final_action") or btc.get("final_tier") or btc.get("final_action") or "-",
        "canonical_reason": btc.get("canonical_final_reason") or btc.get("final_reason_v3") or btc.get("final_reason") or [],
        "confidence": btc.get("trade_plan_confidence") or btc.get("confidence_score"),
        "confidence_direction": btc.get("confidence_direction"),
        "confidence_execution": btc.get("confidence_execution"),
        "confidence_rr": btc.get("confidence_rr"),
        "confidence_external": btc.get("confidence_external"),
        "long_zone": long_zone,
        "long_sl": trade_report.get("long_sl") or btc.get("atr_stop_long"),
        "long_tp1": trade_report.get("long_tp1") or btc.get("target_long_1"),
        "long_tp2": trade_report.get("long_tp2") or btc.get("target_long_2"),
        "short_zone": short_zone,
        "short_sl": trade_report.get("short_sl") or btc.get("atr_stop_short"),
        "short_tp1": trade_report.get("short_tp1") or btc.get("target_short_1"),
        "short_tp2": trade_report.get("short_tp2") or btc.get("target_short_2"),
        "bull_trigger": btc.get("bull_trigger_price"),
        "bear_trigger": btc.get("bear_trigger_price"),
    }


def _zone_text(z):
    z = _as_zone(z)
    return f"{_fmt_num(z[0])} – {_fmt_num(z[1])}" if z else "-"


BASE_HTML = """
<!doctype html>
<html lang="hu"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ title }}</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;background:#0b0f14;color:#e8eef5;margin:0}
nav{display:flex;gap:12px;padding:14px 18px;background:#111827;position:sticky;top:0}
nav a{color:#dbeafe;text-decoration:none;padding:8px 12px;border-radius:10px;background:#1f2937}
main{max-width:1100px;margin:18px auto;padding:0 16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px}
.card{background:#111827;border:1px solid #243041;border-radius:16px;padding:16px}
.h{font-size:13px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px}
.v{font-size:28px;font-weight:700}
.small{font-size:14px;color:#cbd5e1}
.table{width:100%;border-collapse:collapse}
.table td,.table th{padding:10px;border-bottom:1px solid #243041;text-align:left}
.good{color:#22c55e}.bad{color:#ef4444}.muted{color:#94a3b8}
pre{white-space:pre-wrap;word-break:break-word;background:#0f172a;border:1px solid #243041;padding:14px;border-radius:12px}
</style></head><body>
<nav>
<a href="/">Kezdőlap</a>
<a href="/snapshot-view?ts={{ bust }}">Snapshot</a>
<a href="/trade-view?ts={{ bust }}">Trade</a>
<a href="/next15-view?ts={{ bust }}">Next15</a>
</nav>
<main>{{ body|safe }}</main>
</body></html>
"""


def _render(title, body):
    bust = request.args.get("ts", str(int(datetime.now().timestamp())))
    return render_template_string(BASE_HTML, title=title, body=body, bust=html.escape(bust))


@app.route("/")
def home():
    data = load_snapshot() or {}
    model = _trade_view_model(data) if data else {}
    body = f"""
    <div class='grid'>
      <div class='card'><div class='h'>Utolsó snapshot</div><div class='v'>{html.escape(str(model.get('ts','-')))}</div><div class='small'>server_updated_at: {html.escape(str(model.get('server_updated_at','-')))}</div></div>
      <div class='card'><div class='h'>BTC</div><div class='v'>{_fmt_num(model.get('last'))}</div><div class='small'>Verdict: {html.escape(str(model.get('verdict','-')))}</div></div>
      <div class='card'><div class='h'>HTF bias</div><div class='v'>{html.escape(str(model.get('htf_bias','-')).upper())}</div><div class='small'>{html.escape(str(model.get('htf_context','-')))}</div></div>
      <div class='card'><div class='h'>LTF bias</div><div class='v'>{html.escape(str(model.get('ltf_bias','-')).upper())}</div><div class='small'>{html.escape(str(model.get('ltf_context','-')))}</div></div>
    </div>
    """
    return _render("BTC Dashboard", body)


@app.route("/snapshot", methods=["GET"])
def get_snapshot():
    data = load_snapshot()
    if not data:
        return jsonify({"ok": False, "error": "snapshot not uploaded yet"}), 404
    return jsonify(data)


@app.route("/snapshot-pretty", methods=["GET"])
def snapshot_pretty():
    data = load_snapshot()
    if not data:
        return Response("snapshot not uploaded yet", status=404, mimetype="text/plain")
    return Response(json.dumps(data, ensure_ascii=False, indent=2), mimetype="text/plain; charset=utf-8")


@app.route("/snapshot-view", methods=["GET"])
def snapshot_view():
    data = load_snapshot()
    if not data:
        return _render("Snapshot", "<div class='card'>Nincs snapshot feltöltve.</div>")
    body = f"<div class='card'><div class='h'>Snapshot JSON</div><pre>{html.escape(json.dumps(data, ensure_ascii=False, indent=2))}</pre></div>"
    return _render("Snapshot", body)


@app.route("/trade", methods=["GET"])
def trade_json():
    data = load_snapshot()
    if not data:
        return jsonify({"ok": False, "error": "snapshot not uploaded yet"}), 404
    return jsonify(_trade_view_model(data))


@app.route("/trade-pretty", methods=["GET"])
def trade_pretty():
    data = load_snapshot()
    if not data:
        return Response("snapshot not uploaded yet", status=404, mimetype="text/plain")
    return Response(json.dumps(_trade_view_model(data), ensure_ascii=False, indent=2), mimetype="text/plain; charset=utf-8")


@app.route("/trade-view", methods=["GET"])
def trade_view():
    data = load_snapshot()
    if not data:
        return _render("Trade", "<div class='card'>Nincs snapshot feltöltve.</div>")
    m = _trade_view_model(data)
    reasons = m["canonical_reason"] if isinstance(m["canonical_reason"], list) else [m["canonical_reason"]]
    body = f"""
    <div class='grid'>
      <div class='card'><div class='h'>Verdict</div><div class='v'>{html.escape(str(m['verdict']))}</div><div class='small'>canonical: {html.escape(str(m['canonical_action']))}</div></div>
      <div class='card'><div class='h'>HTF bias</div><div class='v'>{html.escape(str(m['htf_bias']).upper())}</div><div class='small'>{html.escape(str(m['htf_context']))}</div></div>
      <div class='card'><div class='h'>LTF bias</div><div class='v'>{html.escape(str(m['ltf_bias']).upper())}</div><div class='small'>{html.escape(str(m['ltf_context']))}</div></div>
      <div class='card'><div class='h'>Confidence</div><div class='v'>{_fmt_num(m['confidence'])}</div><div class='small'>Dir { _fmt_num(m['confidence_direction']) } | Exec { _fmt_num(m['confidence_execution']) } | RR { _fmt_num(m['confidence_rr']) } | Ext { _fmt_num(m['confidence_external']) }</div></div>
    </div>
    <div class='grid' style='margin-top:14px'>
      <div class='card'><div class='h'>Long lehetőség</div>
        <table class='table'>
          <tr><th>Belépő zóna</th><td>{_zone_text(m['long_zone'])}</td></tr>
          <tr><th>SL</th><td>{_fmt_num(m['long_sl'])}</td></tr>
          <tr><th>TP1</th><td>{_fmt_num(m['long_tp1'])}</td></tr>
          <tr><th>TP2</th><td>{_fmt_num(m['long_tp2'])}</td></tr>
        </table>
      </div>
      <div class='card'><div class='h'>Short lehetőség</div>
        <table class='table'>
          <tr><th>Belépő zóna</th><td>{_zone_text(m['short_zone'])}</td></tr>
          <tr><th>SL</th><td>{_fmt_num(m['short_sl'])}</td></tr>
          <tr><th>TP1</th><td>{_fmt_num(m['short_tp1'])}</td></tr>
          <tr><th>TP2</th><td>{_fmt_num(m['short_tp2'])}</td></tr>
        </table>
      </div>
    </div>
    <div class='card' style='margin-top:14px'><div class='h'>Kulcsszintek</div>
      <table class='table'>
        <tr><th>Bull trigger</th><td>{_fmt_num(m['bull_trigger'])}</td><th>Bear trigger</th><td>{_fmt_num(m['bear_trigger'])}</td></tr>
        <tr><th>Indokok</th><td colspan='3'>{html.escape(', '.join(str(x) for x in reasons if x)) or '-'}</td></tr>
      </table>
    </div>
    """
    return _render("Trade", body)


@app.route("/next15", methods=["GET"])
@app.route("/next15-pretty", methods=["GET"])
@app.route("/next15-view", methods=["GET"])
@app.route("/next15/", methods=["GET"])
def next15_alias():
    if request.path.endswith("view") or request.path.endswith("/next15/"):
        return trade_view()
    if request.path.endswith("pretty"):
        return trade_pretty()
    return trade_json()


@app.route("/upload", methods=["POST"])
def upload_snapshot():
    try:
        data = request.get_json(force=True)
        data = add_server_updated_at(data)
        with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return jsonify({"ok": True, "server_updated_at": data.get("server_updated_at")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
