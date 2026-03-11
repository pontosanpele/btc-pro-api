from flask import Flask, jsonify, request, Response
from pathlib import Path
import html
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

app = Flask(__name__)
SNAPSHOT_FILE = Path("snapshot.json")


def load_snapshot() -> Dict[str, Any]:
    if not SNAPSHOT_FILE.exists():
        raise FileNotFoundError("snapshot not uploaded yet")
    with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_snapshot(data: Dict[str, Any]) -> None:
    data = dict(data)
    data["server_updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def pretty_json_text(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False)


def get_btc(data: Dict[str, Any]) -> Dict[str, Any]:
    return data.get("btc", {}) if isinstance(data, dict) else {}


def get_trade_report(btc: Dict[str, Any]) -> Dict[str, Any]:
    tr = btc.get("trade_report", {})
    return tr if isinstance(tr, dict) else {}


def nz(*vals, default="-"):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return default


def fmt_num(v: Any, digits: int = 2) -> str:
    if v is None:
        return "-"
    try:
        return f"{float(v):,.{digits}f}"
    except Exception:
        return str(v)


def fmt_pct(v: Any, digits: int = 2) -> str:
    if v is None:
        return "-"
    try:
        return f"{float(v):.{digits}f}%"
    except Exception:
        return str(v)


def fmt_zone(v: Any, digits: int = 2) -> str:
    if isinstance(v, (list, tuple)) and len(v) == 2:
        return f"{fmt_num(v[0], digits)} – {fmt_num(v[1], digits)}"
    return fmt_num(v, digits) if v is not None else "-"


def fmt_list(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, list):
        return ", ".join(str(x) for x in v) if v else "-"
    return str(v)


def side_class(side: Any) -> str:
    s = str(side or "").lower()
    if "long" in s or s == "bullish":
        return "long"
    if "short" in s or s == "bearish":
        return "short"
    return "neutral"


def nav(current: str) -> str:
    items = [
        ("/snapshot-view", "Snapshot", current == "snapshot"),
        ("/trade-view", "Trade", current == "trade"),
        ("/next15-view", "15 perces", current == "next15"),
    ]
    out = []
    for href, label, active in items:
        cls = "nav-link active" if active else "nav-link"
        out.append(f'<a class="{cls}" href="{href}">{html.escape(label)}</a>')
    return "".join(out)


def shell(title: str, current: str, body: str, extra_head: str = "") -> str:
    return f"""<!doctype html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg:#0b1220; --card:#111827; --card2:#0f172a; --text:#e5e7eb; --muted:#9ca3af;
      --border:#1f2937; --accent:#60a5fa; --green:#16a34a; --greenbg:#052e16; --red:#ef4444; --redbg:#3f0d15;
      --yellow:#f59e0b; --shadow:0 10px 30px rgba(0,0,0,.25);
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:var(--bg); color:var(--text); font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }}
    .wrap {{ max-width:1280px; margin:0 auto; padding:20px; }}
    .top {{ display:flex; justify-content:space-between; gap:16px; align-items:center; flex-wrap:wrap; margin-bottom:18px; }}
    .title {{ font-size:24px; font-weight:800; margin:0; }}
    .sub {{ color:var(--muted); font-size:14px; margin-top:4px; }}
    .nav {{ display:flex; gap:10px; flex-wrap:wrap; }}
    .nav-link {{ color:var(--text); text-decoration:none; border:1px solid var(--border); background:var(--card); padding:10px 14px; border-radius:12px; }}
    .nav-link.active {{ border-color:var(--accent); box-shadow: inset 0 0 0 1px rgba(96,165,250,.15); }}
    .grid {{ display:grid; grid-template-columns:repeat(12,minmax(0,1fr)); gap:16px; }}
    .card {{ background:var(--card); border:1px solid var(--border); border-radius:16px; box-shadow:var(--shadow); overflow:hidden; }}
    .card.pad {{ padding:16px; }}
    .span-12 {{ grid-column:span 12; }} .span-6 {{ grid-column:span 6; }} .span-4 {{ grid-column:span 4; }}
    @media (max-width: 900px) {{ .span-6,.span-4 {{ grid-column:span 12; }} }}
    .kvs {{ display:grid; grid-template-columns: 180px 1fr; gap:10px 14px; align-items:start; }}
    .k {{ color:var(--muted); font-size:13px; }} .v {{ font-weight:600; word-break:break-word; }}
    .badge {{ display:inline-flex; align-items:center; gap:8px; padding:7px 11px; border-radius:999px; font-size:12px; font-weight:700; border:1px solid var(--border); }}
    .badge.long {{ background:rgba(22,163,74,.12); color:#86efac; border-color:rgba(22,163,74,.35); }}
    .badge.short {{ background:rgba(239,68,68,.12); color:#fca5a5; border-color:rgba(239,68,68,.35); }}
    .badge.neutral {{ background:rgba(245,158,11,.12); color:#fde68a; border-color:rgba(245,158,11,.35); }}
    .sect-title {{ font-size:15px; font-weight:800; margin:0 0 12px; }}
    .split {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
    @media (max-width:900px) {{ .split {{ grid-template-columns:1fr; }} }}
    .sidebox {{ border:1px solid var(--border); border-radius:16px; padding:16px; }}
    .sidebox.long {{ background:linear-gradient(180deg, rgba(22,163,74,.10), rgba(22,163,74,.04)); border-color:rgba(22,163,74,.35); }}
    .sidebox.short {{ background:linear-gradient(180deg, rgba(239,68,68,.10), rgba(239,68,68,.04)); border-color:rgba(239,68,68,.35); }}
    .side-title {{ display:flex; align-items:center; gap:10px; font-size:16px; font-weight:800; margin:0 0 12px; }}
    .dot {{ width:10px; height:10px; border-radius:50%; display:inline-block; }}
    .dot.long {{ background:#22c55e; }} .dot.short {{ background:#ef4444; }}
    .mono {{ font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono",monospace; }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin:0 0 12px; }}
    button.copybtn {{ border:1px solid var(--border); background:var(--card2); color:var(--text); padding:10px 14px; border-radius:12px; cursor:pointer; font-weight:700; }}
    button.copybtn:hover {{ border-color:var(--accent); }}
    pre.snapshot {{ margin:0; padding:18px; font-size:13px; white-space:pre-wrap; word-break:break-word; overflow:auto; max-height:70vh; }}
    .muted {{ color:var(--muted); }}
  </style>
  {extra_head}
</head>
<body>
  <div class=\"wrap\">
    <div class=\"top\">
      <div>
        <h1 class=\"title\">BTC Pro</h1>
        <div class=\"sub\">{html.escape(title)}</div>
      </div>
      <div class=\"nav\">{nav(current)}</div>
    </div>
    {body}
  </div>
</body>
</html>"""


def snapshot_page(data: Dict[str, Any]) -> str:
    raw = pretty_json_text(data)
    body = f"""
    <div class=\"grid\">
      <div class=\"card pad span-12\">
        <div class=\"actions\">
          <button class=\"copybtn\" onclick=\"copySnapshot()\">Snapshot másolása</button>
          <span id=\"copyState\" class=\"muted\"></span>
        </div>
        <div class=\"card\"><pre id=\"snapshotText\" class=\"snapshot\">{html.escape(raw)}</pre></div>
      </div>
    </div>
    <script>
      async function copySnapshot() {{
        const txt = document.getElementById('snapshotText').innerText;
        const state = document.getElementById('copyState');
        try {{
          await navigator.clipboard.writeText(txt);
          state.textContent = 'Kimásolva';
          setTimeout(() => state.textContent = '', 1800);
        }} catch (e) {{
          state.textContent = 'Másolás nem sikerült';
          setTimeout(() => state.textContent = '', 1800);
        }}
      }}
    </script>
    """
    return shell("Snapshot nézet", "snapshot", body)


def trade_view_data(data: Dict[str, Any]) -> Dict[str, Any]:
    b = get_btc(data)
    tr = get_trade_report(b)
    return {
        "ts": nz(data.get("ts_bucharest")),
        "price": fmt_num(b.get("last")),
        "summary": nz(b.get("summary_status"), tr.get("verdict"), "WATCH"),
        "htf_bias": nz(b.get("dominant_bias_htf"), tr.get("dominant_bias_htf")),
        "htf_context": nz(b.get("dominant_bias_context"), tr.get("dominant_bias_context")),
        "ltf_bias": nz(b.get("execution_bias_ltf"), tr.get("execution_bias_ltf")),
        "ltf_context": nz(b.get("execution_bias_context"), tr.get("execution_bias_context")),
        "analysis_bias": nz(b.get("analysis_bias"), tr.get("analysis_bias"), b.get("trade_bias"), tr.get("direction")),
        "execution_status": nz(b.get("execution_status"), tr.get("execution_status"), "watching"),
        "trade_plan_status": nz(b.get("trade_plan_status"), tr.get("trade_plan_status"), ("active" if b.get("trade_plan_side") not in (None, "no_trade") else "inactive")),
        "canonical_action": nz(b.get("canonical_final_action"), tr.get("canonical_final_action"), b.get("final_action_v4"), b.get("final_action_v3"), b.get("final_action_v2"), b.get("final_action"), tr.get("verdict"), "WAIT"),
        "canonical_side": nz(b.get("canonical_final_side"), tr.get("canonical_final_side"), b.get("final_side_v4"), b.get("final_side_v3"), b.get("final_side_v2"), b.get("final_side"), tr.get("direction"), "neutral"),
        "canonical_reason": fmt_list(nz(b.get("canonical_final_reason"), tr.get("canonical_final_reason"), b.get("final_reason_v4"), b.get("final_reason_v3"), b.get("final_reason_v2"), b.get("final_reason"), default=[])),
        "orderbook_wall_explanation": nz(b.get("orderbook_wall_explanation"), tr.get("orderbook_wall_explanation")),
        "micro_pressure": nz(b.get("micro_orderbook_pressure"), tr.get("micro_orderbook_pressure")),
        "major_pressure": nz(b.get("major_wall_pressure"), tr.get("major_wall_pressure")),
        "wall_pressure": nz(b.get("wall_pressure_side"), tr.get("wall_pressure_side")),
        "long_zone": fmt_zone(nz(b.get("long_entry_zone"), tr.get("long_entry_zone"), b.get("trade_plan_entry_zone") if b.get("trade_plan_side") == "long" else None)),
        "long_zone_aggr": fmt_zone(nz(b.get("long_entry_zone_aggressive"), tr.get("long_entry_zone_aggressive"))),
        "long_zone_cons": fmt_zone(nz(b.get("long_entry_zone_conservative"), tr.get("long_entry_zone_conservative"))),
        "long_counter": nz(b.get("long_countertrend"), tr.get("long_countertrend"), b.get("long_is_countertrend"), tr.get("long_is_countertrend"), False),
        "long_sl": fmt_num(nz(b.get("long_sl"), tr.get("long_sl"), b.get("trade_plan_stop") if b.get("trade_plan_side") == "long" else None)),
        "long_tp1": fmt_num(nz(b.get("long_tp1"), tr.get("long_tp1"), b.get("trade_plan_t1") if b.get("trade_plan_side") == "long" else None)),
        "long_tp2": fmt_num(nz(b.get("long_tp2"), tr.get("long_tp2"), b.get("trade_plan_t2") if b.get("trade_plan_side") == "long" else None)),
        "short_zone": fmt_zone(nz(b.get("short_entry_zone"), tr.get("short_entry_zone"), b.get("trade_plan_entry_zone") if b.get("trade_plan_side") == "short" else None)),
        "short_zone_aggr": fmt_zone(nz(b.get("short_entry_zone_aggressive"), tr.get("short_entry_zone_aggressive"))),
        "short_zone_cons": fmt_zone(nz(b.get("short_entry_zone_conservative"), tr.get("short_entry_zone_conservative"))),
        "short_counter": nz(b.get("short_countertrend"), tr.get("short_countertrend"), b.get("short_is_countertrend"), tr.get("short_is_countertrend"), False),
        "short_sl": fmt_num(nz(b.get("short_sl"), tr.get("short_sl"), b.get("trade_plan_stop") if b.get("trade_plan_side") == "short" else None)),
        "short_tp1": fmt_num(nz(b.get("short_tp1"), tr.get("short_tp1"), b.get("trade_plan_t1") if b.get("trade_plan_side") == "short" else None)),
        "short_tp2": fmt_num(nz(b.get("short_tp2"), tr.get("short_tp2"), b.get("trade_plan_t2") if b.get("trade_plan_side") == "short" else None)),
    }


def yesno(v: Any) -> str:
    if isinstance(v, bool):
        return "Igen" if v else "Nem"
    if v in ("true", "false"):
        return "Igen" if v == "true" else "Nem"
    return str(v)


def trade_page(data: Dict[str, Any], current: str = "trade") -> str:
    d = trade_view_data(data)
    side_cls = side_class(d["canonical_side"])
    body = f"""
    <div class=\"grid\">
      <div class=\"card pad span-12\">
        <div class=\"split\">
          <div>
            <div class=\"sect-title\">Áttekintés</div>
            <div class=\"kvs\">
              <div class=\"k\">Idő</div><div class=\"v\">{html.escape(str(d['ts']))}</div>
              <div class=\"k\">Ár</div><div class=\"v mono\">{d['price']}</div>
              <div class=\"k\">Canonical action</div><div class=\"v\"><span class=\"badge {side_cls}\">{html.escape(str(d['canonical_action']))}</span></div>
              <div class=\"k\">Canonical side</div><div class=\"v\">{html.escape(str(d['canonical_side']))}</div>
              <div class=\"k\">Canonical reason</div><div class=\"v\">{html.escape(str(d['canonical_reason']))}</div>
              <div class=\"k\">HTF bias</div><div class=\"v\">{html.escape(str(d['htf_bias']))}</div>
              <div class=\"k\">HTF context</div><div class=\"v\">{html.escape(str(d['htf_context']))}</div>
              <div class=\"k\">LTF bias</div><div class=\"v\">{html.escape(str(d['ltf_bias']))}</div>
              <div class=\"k\">LTF context</div><div class=\"v\">{html.escape(str(d['ltf_context']))}</div>
              <div class=\"k\">Analysis bias</div><div class=\"v\">{html.escape(str(d['analysis_bias']))}</div>
              <div class=\"k\">Execution status</div><div class=\"v\">{html.escape(str(d['execution_status']))}</div>
              <div class=\"k\">Trade plan status</div><div class=\"v\">{html.escape(str(d['trade_plan_status']))}</div>
            </div>
          </div>
          <div>
            <div class=\"sect-title\">Orderbook / wall</div>
            <div class=\"kvs\">
              <div class=\"k\">Micro pressure</div><div class=\"v\">{html.escape(str(d['micro_pressure']))}</div>
              <div class=\"k\">Major pressure</div><div class=\"v\">{html.escape(str(d['major_pressure']))}</div>
              <div class=\"k\">Wall pressure</div><div class=\"v\">{html.escape(str(d['wall_pressure']))}</div>
              <div class=\"k\">Magyarázat</div><div class=\"v\">{html.escape(str(d['orderbook_wall_explanation']))}</div>
            </div>
          </div>
        </div>
      </div>
      <div class=\"span-12\">
        <div class=\"split\">
          <div class=\"sidebox long\">
            <div class=\"side-title\"><span class=\"dot long\"></span>Long</div>
            <div class=\"kvs\">
              <div class=\"k\">Fő zóna</div><div class=\"v mono\">{d['long_zone']}</div>
              <div class=\"k\">Agresszív</div><div class=\"v mono\">{d['long_zone_aggr']}</div>
              <div class=\"k\">Konzervatív</div><div class=\"v mono\">{d['long_zone_cons']}</div>
              <div class=\"k\">Countertrend</div><div class=\"v\">{yesno(d['long_counter'])}</div>
              <div class=\"k\">SL</div><div class=\"v mono\">{d['long_sl']}</div>
              <div class=\"k\">TP1</div><div class=\"v mono\">{d['long_tp1']}</div>
              <div class=\"k\">TP2</div><div class=\"v mono\">{d['long_tp2']}</div>
            </div>
          </div>
          <div class=\"sidebox short\">
            <div class=\"side-title\"><span class=\"dot short\"></span>Short</div>
            <div class=\"kvs\">
              <div class=\"k\">Fő zóna</div><div class=\"v mono\">{d['short_zone']}</div>
              <div class=\"k\">Agresszív</div><div class=\"v mono\">{d['short_zone_aggr']}</div>
              <div class=\"k\">Konzervatív</div><div class=\"v mono\">{d['short_zone_cons']}</div>
              <div class=\"k\">Countertrend</div><div class=\"v\">{yesno(d['short_counter'])}</div>
              <div class=\"k\">SL</div><div class=\"v mono\">{d['short_sl']}</div>
              <div class=\"k\">TP1</div><div class=\"v mono\">{d['short_tp1']}</div>
              <div class=\"k\">TP2</div><div class=\"v mono\">{d['short_tp2']}</div>
            </div>
          </div>
        </div>
      </div>
    </div>
    """
    title = "Trade nézet" if current == "trade" else "15 perces trade nézet"
    return shell(title, current, body)


@app.after_request
def add_no_cache_headers(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/")
def home():
    return Response(shell("Kezdőlap", "snapshot", "<div class='card pad'><div class='sect-title'>Menü</div><div class='muted'>Használd a felső menüt: Snapshot, Trade, 15 perces.</div></div>"), mimetype="text/html; charset=utf-8")


@app.route("/snapshot", methods=["GET"])
def get_snapshot():
    if not SNAPSHOT_FILE.exists():
        return jsonify({"ok": False, "error": "snapshot not uploaded yet"}), 404
    try:
        return jsonify(load_snapshot())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/snapshot-pretty", methods=["GET"])
def get_snapshot_pretty():
    if not SNAPSHOT_FILE.exists():
        return Response(json.dumps({"ok": False, "error": "snapshot not uploaded yet"}, ensure_ascii=False, indent=2), status=404, mimetype="text/plain; charset=utf-8")
    try:
        return Response(pretty_json_text(load_snapshot()), mimetype="text/plain; charset=utf-8")
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False, indent=2), status=500, mimetype="text/plain; charset=utf-8")


@app.route("/snapshot-view", methods=["GET"])
def get_snapshot_view():
    if not SNAPSHOT_FILE.exists():
        return Response(shell("Snapshot nézet", "snapshot", "<div class='card pad'>snapshot not uploaded yet</div>"), status=404, mimetype="text/html; charset=utf-8")
    try:
        return Response(snapshot_page(load_snapshot()), mimetype="text/html; charset=utf-8")
    except Exception as e:
        return Response(shell("Hiba", "snapshot", f"<div class='card pad'><pre class='snapshot'>{html.escape(str(e))}</pre></div>"), status=500, mimetype="text/html; charset=utf-8")


@app.route("/trade", methods=["GET"])
@app.route("/trade-pretty", methods=["GET"])
@app.route("/trade-view", methods=["GET"])
def get_trade_view():
    if not SNAPSHOT_FILE.exists():
        return Response(shell("Trade nézet", "trade", "<div class='card pad'>snapshot not uploaded yet</div>"), status=404, mimetype="text/html; charset=utf-8")
    try:
        return Response(trade_page(load_snapshot(), current="trade"), mimetype="text/html; charset=utf-8")
    except Exception as e:
        return Response(shell("Trade hiba", "trade", f"<div class='card pad'><pre class='snapshot'>{html.escape(str(e))}</pre></div>"), status=500, mimetype="text/html; charset=utf-8")


@app.route("/next15", methods=["GET"])
@app.route("/next15-pretty", methods=["GET"])
@app.route("/next15-view", methods=["GET"])
def get_next15_view():
    if not SNAPSHOT_FILE.exists():
        return Response(shell("15 perces nézet", "next15", "<div class='card pad'>snapshot not uploaded yet</div>"), status=404, mimetype="text/html; charset=utf-8")
    try:
        return Response(trade_page(load_snapshot(), current="next15"), mimetype="text/html; charset=utf-8")
    except Exception as e:
        return Response(shell("15 perces hiba", "next15", f"<div class='card pad'><pre class='snapshot'>{html.escape(str(e))}</pre></div>"), status=500, mimetype="text/html; charset=utf-8")


@app.route("/upload", methods=["POST"])
def upload_snapshot():
    try:
        data = request.get_json(force=True)
        save_snapshot(data)
        return jsonify({"ok": True, "server_updated_at": data.get("server_updated_at")})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
