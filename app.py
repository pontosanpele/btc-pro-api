from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import html
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
    resp = make_response(jsonify(payload), status)
    return _apply_no_cache(resp)


def _text_response(text: str, status: int = 200, content_type: str = "text/plain; charset=utf-8"):
    resp = make_response(text, status)
    resp.headers["Content-Type"] = content_type
    return _apply_no_cache(resp)


def _pretty_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _fmt_num(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, (int, float)):
        if abs(v) >= 1000:
            return f"{v:,.2f}".replace(",", " ")
        return f"{v:.2f}"
    return str(v)


def _fmt_zone(v: Any) -> str:
    if isinstance(v, (list, tuple)) and len(v) == 2:
        return f"{_fmt_num(v[0])} – {_fmt_num(v[1])}"
    return _fmt_num(v)


def _extract_trade_view(data: dict[str, Any]) -> dict[str, Any]:
    btc = data.get("btc", {})
    report = btc.get("trade_report", {})
    long_entry = report.get("long_entry_zone") or btc.get("long_entry_zone") or btc.get("trade_plan_entry_zone")
    short_entry = report.get("short_entry_zone") or btc.get("short_entry_zone") or btc.get("trade_plan_entry_zone")
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
            "sl": report.get("long_sl") or btc.get("atr_stop_long") or btc.get("invalidation_long"),
            "tp1": report.get("long_tp1") or btc.get("target_long_1"),
            "tp2": report.get("long_tp2") or btc.get("target_long_2"),
        },
        "short": {
            "entry_zone": short_entry,
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
        "long_entry_zone": btc.get("long_entry_zone"),
        "short_entry_zone": btc.get("short_entry_zone"),
        "reasons": reasons,
    }


def _build_menu_cards(ts: str) -> str:
    cards = [
        ("Snapshot JSON", f"/snapshot?ts={ts}", "Teljes nyers JSON adat."),
        ("Snapshot Pretty", f"/snapshot-pretty?ts={ts}", "Ugyanaz az adat, szépen formázva."),
        ("Snapshot View", f"/snapshot-view?ts={ts}", "Kényelmes böngészős nézet."),
        ("Trade JSON", f"/trade?ts={ts}", "Kivonatolt trade adatok JSON-ban."),
        ("Trade View", f"/trade-view?ts={ts}", "Emberbarát trade jelentés."),
        ("Next15 View", f"/next15-view?ts={ts}", "Rövid, 15 perces bias-jelentés."),
    ]
    return "".join(
        f"<a class='card' href='{href}'><div class='card-title'>{title}</div><div class='card-text'>{text}</div></a>"
        for title, href, text in cards
    )


def _layout(title: str, body: str, active: str = "") -> str:
    ts = html.escape(request.args.get("ts", str(int(datetime.now().timestamp()))))
    nav = [
        ("Főmenü", f"/?ts={ts}", "home"),
        ("Snapshot", f"/snapshot-view?ts={ts}", "snapshot"),
        ("Trade", f"/trade-view?ts={ts}", "trade"),
        ("Next15", f"/next15-view?ts={ts}", "next15"),
    ]
    nav_html = "".join(
        f"<a class='nav-link {'active' if key == active else ''}' href='{href}'>{label}</a>"
        for label, href, key in nav
    )
    return f"""<!doctype html>
<html lang='hu'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<meta http-equiv='Cache-Control' content='no-store, no-cache, must-revalidate, max-age=0'>
<meta http-equiv='Pragma' content='no-cache'>
<meta http-equiv='Expires' content='0'>
<title>{html.escape(title)}</title>
<style>
:root{{--bg:#0b1220;--panel:#121c2e;--panel2:#17243b;--text:#e8eefc;--muted:#9fb0d1;--accent:#62a5ff;--green:#3ddc97;--red:#ff6b6b;--border:#263754}}
*{{box-sizing:border-box}}
body{{margin:0;font-family:Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:linear-gradient(180deg,#09111d,#0e1727 35%,#0b1220);color:var(--text)}}
a{{color:inherit;text-decoration:none}}
.wrap{{max-width:1180px;margin:0 auto;padding:24px}}
.topbar{{display:flex;justify-content:space-between;align-items:center;gap:16px;padding:14px 18px;background:rgba(18,28,46,.85);backdrop-filter:blur(10px);border:1px solid var(--border);border-radius:18px;position:sticky;top:14px;z-index:10}}
.brand{{font-weight:800;letter-spacing:.2px}}
.brand small{{display:block;color:var(--muted);font-weight:600;margin-top:2px}}
.nav{{display:flex;gap:10px;flex-wrap:wrap}}
.nav-link{{padding:10px 14px;border-radius:12px;background:var(--panel2);border:1px solid var(--border);color:var(--muted);font-weight:700}}
.nav-link.active,.nav-link:hover{{color:var(--text);border-color:var(--accent)}}
.hero{{display:grid;grid-template-columns:1.3fr .7fr;gap:18px;margin-top:18px}}
.panel{{background:rgba(18,28,46,.9);border:1px solid var(--border);border-radius:22px;padding:22px;box-shadow:0 12px 40px rgba(0,0,0,.25)}}
.h1{{font-size:30px;font-weight:900;margin:0 0 8px}}
.sub{{color:var(--muted);line-height:1.5}}
.badge{{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;background:#0f1a2c;border:1px solid var(--border);font-size:13px;color:var(--muted);font-weight:700}}
.grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin-top:18px}}
.card{{display:block;padding:16px;border-radius:18px;background:var(--panel2);border:1px solid var(--border);transition:.18s transform,.18s border-color,.18s background}}
.card:hover{{transform:translateY(-2px);border-color:var(--accent);background:#1a2944}}
.card-title{{font-weight:800;margin-bottom:6px}}
.card-text{{color:var(--muted);font-size:14px;line-height:1.45}}
.stat{{display:grid;gap:10px}}
.kv{{display:flex;justify-content:space-between;gap:12px;padding:12px 14px;background:#0f1728;border:1px solid var(--border);border-radius:14px}}
.kv .k{{color:var(--muted)}}
.section-title{{font-size:18px;font-weight:800;margin:0 0 14px}}
.table{{width:100%;border-collapse:separate;border-spacing:0 10px}}
.table td{{padding:12px 14px;background:#0f1728;border-top:1px solid var(--border);border-bottom:1px solid var(--border)}}
.table td:first-child{{border-left:1px solid var(--border);border-radius:12px 0 0 12px;color:var(--muted);width:34%}}
.table td:last-child{{border-right:1px solid var(--border);border-radius:0 12px 12px 0;font-weight:700}}
.codebox{{background:#0a1120;border:1px solid var(--border);border-radius:18px;padding:18px;overflow:auto}}
pre{{margin:0;white-space:pre-wrap;word-break:break-word;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#d7e5ff}}
.banner{{margin-top:18px;padding:14px 18px;border-radius:16px;background:linear-gradient(90deg,rgba(98,165,255,.16),rgba(61,220,151,.12));border:1px solid var(--border);color:var(--muted)}}
.duo{{display:grid;grid-template-columns:1fr 1fr;gap:18px}}
.long{{color:var(--green)}} .short{{color:var(--red)}}
@media (max-width:900px){{.hero,.duo,.grid{{grid-template-columns:1fr}} .wrap{{padding:16px}} .topbar{{position:static}}}}
</style>
</head>
<body>
<div class='wrap'>
  <div class='topbar'>
    <div class='brand'>BTC Snapshot Dashboard<small>Cache-busting aktív • ts={ts}</small></div>
    <div class='nav'>{nav_html}</div>
  </div>
  {body}
</div>
</body>
</html>"""


def _home_html() -> str:
    ts = html.escape(request.args.get("ts", str(int(datetime.now().timestamp()))))
    body = f"""
    <div class='hero'>
      <div class='panel'>
        <div class='badge'>● Live snapshot szerver</div>
        <h1 class='h1'>Áttekinthetőbb menü és emberbarát nézetek</h1>
        <div class='sub'>Itt egy helyen eléred a teljes snapshotot, a trade kivonatot és a rövid next15 riportot. A cache-bustinghez elég minden megnyitásnál más <b>ts</b> query paramot használni.</div>
        <div class='banner'>Példa: <b>/trade-view?ts={ts}</b> vagy <b>/snapshot-view?ts={ts}</b></div>
      </div>
      <div class='panel stat'>
        <div class='section-title'>Gyors linkek</div>
        <div class='kv'><span class='k'>Snapshot</span><span>/snapshot-view</span></div>
        <div class='kv'><span class='k'>Trade</span><span>/trade-view</span></div>
        <div class='kv'><span class='k'>Next15</span><span>/next15-view</span></div>
        <div class='kv'><span class='k'>Upload</span><span>POST /upload</span></div>
      </div>
    </div>
    <div class='grid'>{_build_menu_cards(ts)}</div>
    """
    return _layout("BTC Snapshot Dashboard", body, "home")


def _render_trade_card(title: str, side_class: str, payload: dict[str, Any]) -> str:
    return f"""
    <div class='panel'>
      <div class='section-title {side_class}'>{html.escape(title)}</div>
      <table class='table'>
        <tr><td>Belépő zóna</td><td>{html.escape(_fmt_zone(payload.get('entry_zone')))}</td></tr>
        <tr><td>SL</td><td>{html.escape(_fmt_num(payload.get('sl')))}</td></tr>
        <tr><td>TP1</td><td>{html.escape(_fmt_num(payload.get('tp1')))}</td></tr>
        <tr><td>TP2</td><td>{html.escape(_fmt_num(payload.get('tp2')))}</td></tr>
      </table>
    </div>
    """


def _trade_html(payload: dict[str, Any], title: str) -> str:
    key_levels = payload.get("key_levels") or {}
    body = f"""
    <div class='hero'>
      <div class='panel'>
        <div class='badge'>Trade jelentés</div>
        <h1 class='h1'>{html.escape(title)}</h1>
        <div class='sub'>Emberbarát rövid kivonat a snapshotból: verdict, bias, entry zónák, SL/TP szintek és kulcsszintek.</div>
      </div>
      <div class='panel stat'>
        <div class='section-title'>Állapot</div>
        <div class='kv'><span class='k'>Időpont</span><span>{html.escape(str(payload.get('ts_bucharest') or '-'))}</span></div>
        <div class='kv'><span class='k'>Szerver frissítve</span><span>{html.escape(str(payload.get('server_updated_at') or '-'))}</span></div>
        <div class='kv'><span class='k'>Ár</span><span>{html.escape(_fmt_num(payload.get('price')))}</span></div>
        <div class='kv'><span class='k'>Verdict</span><span>{html.escape(str(payload.get('verdict') or '-'))}</span></div>
        <div class='kv'><span class='k'>Irány</span><span>{html.escape(str(payload.get('direction') or '-'))}</span></div>
        <div class='kv'><span class='k'>HTF bias</span><span>{html.escape(str(payload.get('dominant_bias_htf') or '-'))}</span></div>
        <div class='kv'><span class='k'>LTF execution</span><span>{html.escape(str(payload.get('execution_bias_ltf') or '-'))}</span></div>
      </div>
    </div>
    <div class='duo' style='margin-top:18px'>
      {_render_trade_card('Long setup', 'long', payload.get('long') or {})}
      {_render_trade_card('Short setup', 'short', payload.get('short') or {})}
    </div>
    <div class='panel' style='margin-top:18px'>
      <div class='section-title'>Kulcsszintek</div>
      <table class='table'>
        {''.join(f"<tr><td>{html.escape(str(k))}</td><td>{html.escape(_fmt_zone(v))}</td></tr>" for k, v in key_levels.items())}
      </table>
    </div>
    """
    return _layout(title, body, "trade")


def _next15_html(payload: dict[str, Any]) -> str:
    reasons = payload.get("reasons") or []
    reason_rows = "".join(f"<tr><td>Ok</td><td>{html.escape(str(r))}</td></tr>" for r in reasons) or "<tr><td>Ok</td><td>-</td></tr>"
    body = f"""
    <div class='hero'>
      <div class='panel'>
        <div class='badge'>Next 15 minutes</div>
        <h1 class='h1'>Rövid 15 perces bias-jelentés</h1>
        <div class='sub'>Gyors áttekintés a következő percekre: bias, confidence, trigger szintek és indokok.</div>
      </div>
      <div class='panel stat'>
        <div class='section-title'>Összegzés</div>
        <div class='kv'><span class='k'>Időpont</span><span>{html.escape(str(payload.get('ts_bucharest') or '-'))}</span></div>
        <div class='kv'><span class='k'>Bias</span><span>{html.escape(str(payload.get('next_15m_bias') or '-'))}</span></div>
        <div class='kv'><span class='k'>Confidence</span><span>{html.escape(_fmt_num(payload.get('confidence')))}</span></div>
        <div class='kv'><span class='k'>Verdict</span><span>{html.escape(str(payload.get('verdict') or '-'))}</span></div>
        <div class='kv'><span class='k'>Bull trigger</span><span>{html.escape(_fmt_num(payload.get('bull_trigger')))}</span></div>
        <div class='kv'><span class='k'>Bear trigger</span><span>{html.escape(_fmt_num(payload.get('bear_trigger')))}</span></div>
      </div>
    </div>
    <div class='duo' style='margin-top:18px'>
      <div class='panel'>
        <div class='section-title long'>Long lehetőség</div>
        <table class='table'>
          <tr><td>Belépő zóna</td><td>{html.escape(_fmt_zone(payload.get('long_entry_zone')))}</td></tr>
        </table>
      </div>
      <div class='panel'>
        <div class='section-title short'>Short lehetőség</div>
        <table class='table'>
          <tr><td>Belépő zóna</td><td>{html.escape(_fmt_zone(payload.get('short_entry_zone')))}</td></tr>
        </table>
      </div>
    </div>
    <div class='panel' style='margin-top:18px'>
      <div class='section-title'>Indokok</div>
      <table class='table'>{reason_rows}</table>
    </div>
    """
    return _layout("Next15 View", body, "next15")


def _raw_json_html(title: str, payload: Any, active: str) -> str:
    body = f"""
    <div class='hero'>
      <div class='panel'>
        <div class='badge'>Nyers adat</div>
        <h1 class='h1'>{html.escape(title)}</h1>
        <div class='sub'>Formázott JSON nézet böngészőben. Ha biztosra akarsz menni frissítéskor, adj hozzá új <b>ts</b> query paramot.</div>
      </div>
      <div class='panel stat'>
        <div class='section-title'>Tippek</div>
        <div class='kv'><span class='k'>Pretty text</span><span>/snapshot-pretty</span></div>
        <div class='kv'><span class='k'>Trade view</span><span>/trade-view</span></div>
        <div class='kv'><span class='k'>Next15 view</span><span>/next15-view</span></div>
      </div>
    </div>
    <div class='panel' style='margin-top:18px'>
      <div class='codebox'><pre>{html.escape(_pretty_json(payload))}</pre></div>
    </div>
    """
    return _layout(title, body, active)


@app.route("/")
def home():
    return _text_response(_home_html(), content_type="text/html; charset=utf-8")


@app.route("/snapshot", methods=["GET"])
def get_snapshot():
    try:
        return _json_response(_load_snapshot())
    except FileNotFoundError as e:
        return _json_response({"ok": False, "error": str(e)}, 404)
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)}, 500)


@app.route("/snapshot-pretty", methods=["GET"])
def get_snapshot_pretty():
    try:
        return _text_response(_pretty_json(_load_snapshot()))
    except FileNotFoundError as e:
        return _text_response(_pretty_json({"ok": False, "error": str(e)}), 404)
    except Exception as e:
        return _text_response(_pretty_json({"ok": False, "error": str(e)}), 500)


@app.route("/snapshot-view", methods=["GET"])
def get_snapshot_view():
    try:
        return _text_response(_raw_json_html("Snapshot View", _load_snapshot(), "snapshot"), content_type="text/html; charset=utf-8")
    except FileNotFoundError as e:
        return _text_response(_raw_json_html("Snapshot View", {"ok": False, "error": str(e)}, "snapshot"), 404, "text/html; charset=utf-8")
    except Exception as e:
        return _text_response(_raw_json_html("Snapshot View", {"ok": False, "error": str(e)}, "snapshot"), 500, "text/html; charset=utf-8")


@app.route("/trade", methods=["GET"])
@app.route("/next15", methods=["GET"])
def get_trade_json():
    try:
        data = _load_snapshot()
        payload = _extract_next15(data) if request.path.endswith("next15") else _extract_trade_view(data)
        return _json_response(payload)
    except FileNotFoundError as e:
        return _json_response({"ok": False, "error": str(e)}, 404)
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)}, 500)


@app.route("/trade-pretty", methods=["GET"])
@app.route("/next15-pretty", methods=["GET"])
def get_trade_pretty():
    try:
        data = _load_snapshot()
        payload = _extract_next15(data) if request.path.endswith("next15-pretty") else _extract_trade_view(data)
        return _text_response(_pretty_json(payload))
    except FileNotFoundError as e:
        return _text_response(_pretty_json({"ok": False, "error": str(e)}), 404)
    except Exception as e:
        return _text_response(_pretty_json({"ok": False, "error": str(e)}), 500)


@app.route("/trade-view", methods=["GET"])
@app.route("/next15-view", methods=["GET"])
def get_trade_view_html():
    try:
        data = _load_snapshot()
        if request.path.endswith("next15-view"):
            return _text_response(_next15_html(_extract_next15(data)), content_type="text/html; charset=utf-8")
        return _text_response(_trade_html(_extract_trade_view(data), "Trade View"), content_type="text/html; charset=utf-8")
    except FileNotFoundError as e:
        return _text_response(_raw_json_html("View", {"ok": False, "error": str(e)}, "trade"), 404, "text/html; charset=utf-8")
    except Exception as e:
        return _text_response(_raw_json_html("View", {"ok": False, "error": str(e)}, "trade"), 500, "text/html; charset=utf-8")


@app.route("/upload", methods=["POST"])
def upload_snapshot():
    try:
        data = request.get_json(force=True)
        if not isinstance(data, dict):
            return _json_response({"ok": False, "error": "JSON object expected"}, 400)
        _save_snapshot(data)
        return _json_response({"ok": True, "server_updated_at": data.get("server_updated_at")})
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)}, 500)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
