
from flask import Flask, jsonify, request, Response
from pathlib import Path
from datetime import datetime, timezone
import json
import html

app = Flask(__name__)
SNAPSHOT_FILE = Path("snapshot.json")


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    resp.headers["Surrogate-Control"] = "no-store"
    resp.headers["Vary"] = "Accept, Accept-Encoding"
    return resp


@app.after_request
def add_cache_headers(resp):
    return _no_cache(resp)


def _bust():
    return request.args.get("ts") or request.args.get("v") or request.args.get("cb") or "1"


def _esc(x):
    return html.escape("" if x is None else str(x))


def _round(v, n=2):
    try:
        return round(float(v), n)
    except Exception:
        return None


def _fmt_num(v):
    r = _round(v)
    return "-" if r is None else f"{r}"


def _fmt_zone(z):
    if isinstance(z, (list, tuple)) and len(z) >= 2:
        a = _round(z[0]); b = _round(z[1])
        if a is not None and b is not None:
            return f"{a} – {b}"
    return "-"


def _pick(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _load_snapshot():
    if not SNAPSHOT_FILE.exists():
        return None, (jsonify({"ok": False, "error": "snapshot not uploaded yet"}), 404)
    try:
        with SNAPSHOT_FILE.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except Exception as e:
        return None, (jsonify({"ok": False, "error": str(e)}), 500)


def _fallback_zone(trigger, atr):
    if trigger is None:
        return None
    try:
        trigger = float(trigger)
        atr = float(atr or 0)
        half = max(atr * 0.08, trigger * 0.00025)
        return [_round(trigger - half), _round(trigger + half)]
    except Exception:
        return None


def _pick_entry_zone(side, btc, report):
    direct = report.get(f"{side}_entry_zone")
    if isinstance(direct, (list, tuple)) and len(direct) >= 2:
        return [_round(direct[0]), _round(direct[1])]

    direct = btc.get(f"{side}_entry_zone")
    if isinstance(direct, (list, tuple)) and len(direct) >= 2:
        return [_round(direct[0]), _round(direct[1])]

    trade_side = btc.get("trade_plan_side")
    plan_zone = btc.get("trade_plan_entry_zone")
    if trade_side == side and isinstance(plan_zone, (list, tuple)) and len(plan_zone) >= 2:
        return [_round(plan_zone[0]), _round(plan_zone[1])]

    if side == "long":
        support = report.get("key_levels", {}).get("support_zone") or btc.get("support_zone")
        if isinstance(support, (list, tuple)) and len(support) >= 2:
            return [_round(support[0]), _round(support[1])]
        trigger = btc.get("bull_trigger_price")
    else:
        resistance = report.get("key_levels", {}).get("resistance_zone") or btc.get("resistance_zone")
        if isinstance(resistance, (list, tuple)) and len(resistance) >= 2:
            return [_round(resistance[0]), _round(resistance[1])]
        trigger = btc.get("bear_trigger_price")

    return _fallback_zone(trigger, btc.get("atr_5m"))


def _extract_trade(data):
    btc = data.get("btc", {}) if isinstance(data, dict) else {}
    report = btc.get("trade_report", {}) if isinstance(btc, dict) else {}
    long_entry = _pick_entry_zone("long", btc, report)
    short_entry = _pick_entry_zone("short", btc, report)
    return {
        "ts_bucharest": data.get("ts_bucharest"),
        "server_updated_at": data.get("server_updated_at"),
        "price": btc.get("last"),
        "direction": report.get("direction") or btc.get("final_side_v4") or btc.get("final_side") or btc.get("trade_bias"),
        "verdict": report.get("verdict") or btc.get("summary_status") or btc.get("final_action_v4") or btc.get("final_action"),
        "market_regime": btc.get("market_regime"),
        "dominant_bias_htf": btc.get("dominant_bias_htf"),
        "execution_bias_ltf": btc.get("execution_bias_ltf"),
        "long": {
            "entry_zone": long_entry,
            "entry_zone_text": _fmt_zone(long_entry),
            "sl": _round(report.get("long_sl") or btc.get("atr_stop_long") or btc.get("invalidation_long")),
            "tp1": _round(report.get("long_tp1") or btc.get("target_long_1")),
            "tp2": _round(report.get("long_tp2") or btc.get("target_long_2")),
            "ready": bool(btc.get("retest_long_ready", False)),
            "score": _round(btc.get("retest_long_score")),
        },
        "short": {
            "entry_zone": short_entry,
            "entry_zone_text": _fmt_zone(short_entry),
            "sl": _round(report.get("short_sl") or btc.get("atr_stop_short") or btc.get("invalidation_short")),
            "tp1": _round(report.get("short_tp1") or btc.get("target_short_1")),
            "tp2": _round(report.get("short_tp2") or btc.get("target_short_2")),
            "ready": bool(btc.get("retest_short_ready", False)),
            "score": _round(btc.get("retest_short_score")),
        },
        "key_levels": report.get("key_levels") or {
            "bull_trigger": btc.get("bull_trigger_price"),
            "bear_trigger": btc.get("bear_trigger_price"),
            "liq_above_1": btc.get("liq_above_1"),
            "liq_below_1": btc.get("liq_below_1"),
        },
    }


def _extract_next15(data):
    btc = data.get("btc", {}) if isinstance(data, dict) else {}
    direction = btc.get("execution_bias_ltf") or btc.get("final_side_v4") or btc.get("final_side") or btc.get("trade_bias") or "neutral"
    reasons = []
    for key in ["dominant_bias_htf", "execution_bias_ltf", "market_regime", "breakout_direction", "wall_pressure_side"]:
        val = btc.get(key)
        if val not in (None, "", False, "neutral"):
            reasons.append(f"{key}: {val}")
    if btc.get("retest_long_ready"):
        reasons.append("retest_long_ready")
    if btc.get("retest_short_ready"):
        reasons.append("retest_short_ready")
    return {
        "ts_bucharest": data.get("ts_bucharest"),
        "server_updated_at": data.get("server_updated_at"),
        "next_15m_bias": direction,
        "confidence": _round(btc.get("confidence_score") or btc.get("trade_plan_confidence")),
        "verdict": btc.get("summary_status") or btc.get("final_action_v4") or btc.get("final_action") or btc.get("trade_bias"),
        "bull_trigger": _round(btc.get("bull_trigger_price")),
        "bear_trigger": _round(btc.get("bear_trigger_price")),
        "long_entry_zone": _fmt_zone(_pick_entry_zone("long", btc, btc.get("trade_report", {}))),
        "short_entry_zone": _fmt_zone(_pick_entry_zone("short", btc, btc.get("trade_report", {}))),
        "reasons": reasons,
    }


def _pretty_text(obj):
    return Response(json.dumps(obj, ensure_ascii=False, indent=2), mimetype="text/plain; charset=utf-8")


def _nav():
    ts = _esc(_bust())
    return (
        "<nav class='nav'>"
        f"<a href='/?ts={ts}'>Kezdőlap</a>"
        f"<a href='/snapshot-view?ts={ts}'>Snapshot</a>"
        f"<a href='/trade-view?ts={ts}'>Trade</a>"
        f"<a href='/next15-view?ts={ts}'>Köv. 15p</a>"
        "</nav>"
    )


def _layout(title, body):
    bust = _esc(_bust())
    return f"""<!doctype html>
<html lang='hu'>
<head>
<meta charset='utf-8'>
<meta http-equiv='Cache-Control' content='no-store, no-cache, must-revalidate, max-age=0'>
<meta http-equiv='Pragma' content='no-cache'>
<meta http-equiv='Expires' content='0'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>{_esc(title)}</title>
<style>
:root{{--bg:#0b1220;--card:#111827;--card2:#1f2937;--txt:#e5e7eb;--muted:#9ca3af;--green:#16a34a;--red:#dc2626;--blue:#2563eb;}}
*{{box-sizing:border-box}} body{{margin:0;background:linear-gradient(180deg,#0b1220,#111827);color:var(--txt);font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif}}
.wrap{{max-width:1180px;margin:0 auto;padding:20px}}
.nav{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:18px}}
.nav a{{color:#fff;text-decoration:none;background:#1d4ed8;padding:10px 14px;border-radius:12px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px}}
.card{{background:rgba(17,24,39,.92);border:1px solid #243041;border-radius:18px;padding:18px;box-shadow:0 8px 30px rgba(0,0,0,.25)}}
h1,h2,h3{{margin:0 0 12px}} .muted{{color:var(--muted)}} .pill{{display:inline-block;padding:6px 10px;border-radius:999px;background:#0f2747;border:1px solid #29466f}}
.good{{color:#86efac}} .bad{{color:#fca5a5}} .mono{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}}
table{{width:100%;border-collapse:collapse}} td{{padding:8px 0;border-bottom:1px solid #253243;vertical-align:top}}
.small{{font-size:13px;color:var(--muted)}}
pre{{white-space:pre-wrap;word-break:break-word;background:#0f172a;padding:14px;border-radius:14px;border:1px solid #223049}}
.footer{{margin-top:20px;color:#9ca3af;font-size:12px}}
</style>
</head>
<body>
<div class='wrap'>
{_nav()}
<div class='small'>cache-bust: <span class='mono'>{bust}</span></div>
{body}
<div class='footer'>BTC Pro UI</div>
</div>
</body></html>"""


@app.route("/")
def home():
    data, err = _load_snapshot()
    if err:
        return err
    btc = data.get("btc", {})
    body = f"""
    <div class='grid'>
      <section class='card'>
        <h1>BTC Pro Dashboard</h1>
        <div class='muted'>Időpont: {_esc(data.get('ts_bucharest'))}</div>
        <div class='muted'>Server updated: {_esc(data.get('server_updated_at'))}</div>
      </section>
      <section class='card'>
        <h2>Piac</h2>
        <table>
          <tr><td>Ár</td><td class='mono'>{_fmt_num(btc.get('last'))}</td></tr>
          <tr><td>Trade bias</td><td>{_esc(btc.get('trade_bias'))}</td></tr>
          <tr><td>HTF bias</td><td>{_esc(btc.get('dominant_bias_htf'))}</td></tr>
          <tr><td>LTF bias</td><td>{_esc(btc.get('execution_bias_ltf'))}</td></tr>
        </table>
      </section>
      <section class='card'>
        <h2>Gyors linkek</h2>
        <div class='nav'>
          <a href='/snapshot-view?ts={_esc(_bust())}'>Snapshot nézet</a>
          <a href='/trade-view?ts={_esc(_bust())}'>Trade nézet</a>
          <a href='/next15-view?ts={_esc(_bust())}'>Következő 15p</a>
        </div>
      </section>
    </div>"""
    return _layout("BTC Pro Dashboard", body)


@app.route("/snapshot")
def snapshot():
    data, err = _load_snapshot()
    return err or jsonify(data)


@app.route("/snapshot-pretty")
def snapshot_pretty():
    data, err = _load_snapshot()
    return err or _pretty_text(data)


@app.route("/snapshot-view")
def snapshot_view():
    data, err = _load_snapshot()
    if err:
        return err
    body = f"""
    <section class='card'>
      <h1>Snapshot</h1>
      <div class='muted'>Időpont: {_esc(data.get('ts_bucharest'))}</div>
      <div class='muted'>Server updated: {_esc(data.get('server_updated_at'))}</div>
      <pre>{_esc(json.dumps(data, ensure_ascii=False, indent=2))}</pre>
    </section>"""
    return _layout("Snapshot", body)


@app.route("/trade")
def trade():
    data, err = _load_snapshot()
    return err or jsonify(_extract_trade(data))


@app.route("/trade-pretty")
def trade_pretty():
    data, err = _load_snapshot()
    return err or _pretty_text(_extract_trade(data))


@app.route("/trade-view")
def trade_view():
    data, err = _load_snapshot()
    if err:
        return err
    t = _extract_trade(data)
    long_block = t["long"]; short_block = t["short"]
    body = f"""
    <div class='grid'>
      <section class='card'>
        <h1>Trade jelentés</h1>
        <table>
          <tr><td>Időpont</td><td>{_esc(t.get('ts_bucharest'))}</td></tr>
          <tr><td>Ár</td><td class='mono'>{_fmt_num(t.get('price'))}</td></tr>
          <tr><td>Irány</td><td>{_esc(t.get('direction'))}</td></tr>
          <tr><td>Verdict</td><td><span class='pill'>{_esc(t.get('verdict'))}</span></td></tr>
          <tr><td>Regime</td><td>{_esc(t.get('market_regime'))}</td></tr>
          <tr><td>HTF bias</td><td>{_esc(t.get('dominant_bias_htf'))}</td></tr>
          <tr><td>LTF bias</td><td>{_esc(t.get('execution_bias_ltf'))}</td></tr>
        </table>
      </section>
      <section class='card'>
        <h2 class='good'>Long</h2>
        <table>
          <tr><td>Belépő zóna</td><td class='mono'>{_esc(long_block['entry_zone_text'])}</td></tr>
          <tr><td>SL</td><td class='mono'>{_fmt_num(long_block['sl'])}</td></tr>
          <tr><td>TP1</td><td class='mono'>{_fmt_num(long_block['tp1'])}</td></tr>
          <tr><td>TP2</td><td class='mono'>{_fmt_num(long_block['tp2'])}</td></tr>
          <tr><td>Ready</td><td>{_esc(long_block['ready'])}</td></tr>
          <tr><td>Score</td><td class='mono'>{_fmt_num(long_block['score'])}</td></tr>
        </table>
      </section>
      <section class='card'>
        <h2 class='bad'>Short</h2>
        <table>
          <tr><td>Belépő zóna</td><td class='mono'>{_esc(short_block['entry_zone_text'])}</td></tr>
          <tr><td>SL</td><td class='mono'>{_fmt_num(short_block['sl'])}</td></tr>
          <tr><td>TP1</td><td class='mono'>{_fmt_num(short_block['tp1'])}</td></tr>
          <tr><td>TP2</td><td class='mono'>{_fmt_num(short_block['tp2'])}</td></tr>
          <tr><td>Ready</td><td>{_esc(short_block['ready'])}</td></tr>
          <tr><td>Score</td><td class='mono'>{_fmt_num(short_block['score'])}</td></tr>
        </table>
      </section>
    </div>"""
    return _layout("Trade jelentés", body)


@app.route("/next15")
def next15():
    data, err = _load_snapshot()
    return err or jsonify(_extract_next15(data))


@app.route("/next15-pretty")
def next15_pretty():
    data, err = _load_snapshot()
    return err or _pretty_text(_extract_next15(data))


@app.route("/next15-view")
def next15_view():
    data, err = _load_snapshot()
    if err:
        return err
    n = _extract_next15(data)
    reasons = "".join(f"<li>{_esc(r)}</li>" for r in n.get("reasons", []))
    body = f"""
    <div class='grid'>
      <section class='card'>
        <h1>Következő 15 perc</h1>
        <table>
          <tr><td>Időpont</td><td>{_esc(n.get('ts_bucharest'))}</td></tr>
          <tr><td>Bias</td><td>{_esc(n.get('next_15m_bias'))}</td></tr>
          <tr><td>Confidence</td><td class='mono'>{_fmt_num(n.get('confidence'))}</td></tr>
          <tr><td>Verdict</td><td>{_esc(n.get('verdict'))}</td></tr>
          <tr><td>Bull trigger</td><td class='mono'>{_fmt_num(n.get('bull_trigger'))}</td></tr>
          <tr><td>Bear trigger</td><td class='mono'>{_fmt_num(n.get('bear_trigger'))}</td></tr>
          <tr><td>Long zóna</td><td class='mono'>{_esc(n.get('long_entry_zone'))}</td></tr>
          <tr><td>Short zóna</td><td class='mono'>{_esc(n.get('short_entry_zone'))}</td></tr>
        </table>
      </section>
      <section class='card'>
        <h2>Indokok</h2>
        <ul>{reasons}</ul>
      </section>
    </div>"""
    return _layout("Következő 15 perc", body)


@app.route("/upload", methods=["POST"])
def upload():
    try:
        payload = request.get_json(force=True, silent=False)
        if not isinstance(payload, dict):
            return jsonify({"ok": False, "error": "JSON object expected"}), 400
        payload["server_updated_at"] = _utc_now_iso()
        with SNAPSHOT_FILE.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        return jsonify({"ok": True, "saved": True, "server_updated_at": payload["server_updated_at"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
