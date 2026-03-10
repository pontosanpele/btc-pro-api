from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import Flask, Response, jsonify, request

app = Flask(__name__)
SNAPSHOT_FILE = Path("snapshot.json")


# -----------------------------
# Core helpers
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def no_cache(response: Response) -> Response:
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@app.after_request
def add_no_cache_headers(response: Response) -> Response:
    return no_cache(response)


def load_snapshot() -> Dict[str, Any]:
    if not SNAPSHOT_FILE.exists():
        raise FileNotFoundError("snapshot not uploaded yet")
    with SNAPSHOT_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_snapshot(data: Dict[str, Any]) -> None:
    with SNAPSHOT_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def pretty_json_text(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False)


def as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def fmt_num(value: Any, digits: int = 2) -> str:
    num = as_float(value)
    if num is None:
        return "-"
    return f"{num:.{digits}f}"


def fmt_pct(value: Any, digits: int = 2) -> str:
    num = as_float(value)
    if num is None:
        return "-"
    return f"{num:.{digits}f}%"


# -----------------------------
# Report extraction
# -----------------------------
def build_next15_report(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    btc = snapshot.get("btc", {})
    trade_report = btc.get("trade_report", {}) if isinstance(btc.get("trade_report"), dict) else {}

    ts = snapshot.get("ts_bucharest")
    last = btc.get("last")
    market_regime = btc.get("market_regime")
    market_bias = btc.get("market_bias")
    market_read = btc.get("market_read")
    trend_5m = btc.get("trend_5m")
    trend_15m = btc.get("trend_15m")
    trend_1h = btc.get("trend_1h")
    summary_status = btc.get("summary_status")
    final_action = btc.get("final_action_v4") or btc.get("final_action_v3") or btc.get("final_action")
    final_side = btc.get("final_side_v4") or btc.get("final_side_v3") or btc.get("final_side")
    retest_winner = btc.get("retest_winner_side")

    # Current next-15 bias
    bias = "neutral"
    if final_side in {"long", "short"}:
        bias = final_side
    elif retest_winner in {"long", "short"}:
        bias = retest_winner
    elif market_bias in {"long", "short"}:
        bias = market_bias

    long_ready = bool(btc.get("retest_long_ready") or btc.get("long_path_valid"))
    short_ready = bool(btc.get("retest_short_ready") or btc.get("short_path_valid"))

    long_entry_zone = trade_report.get("long_entry_zone") or btc.get("long_entry_zone") or btc.get("trade_plan_entry_zone")
    short_entry_zone = trade_report.get("short_entry_zone") or btc.get("short_entry_zone")

    long_sl = trade_report.get("long_sl") or btc.get("trade_plan_stop") or btc.get("atr_stop_long") or btc.get("invalidation_long")
    short_sl = trade_report.get("short_sl") or btc.get("trade_plan_stop") or btc.get("atr_stop_short") or btc.get("invalidation_short")

    long_tp1 = trade_report.get("long_tp1") or btc.get("trade_plan_t1") or btc.get("target_long_1")
    long_tp2 = trade_report.get("long_tp2") or btc.get("trade_plan_t2") or btc.get("target_long_2")
    short_tp1 = trade_report.get("short_tp1") or btc.get("target_short_1")
    short_tp2 = trade_report.get("short_tp2") or btc.get("target_short_2")

    bull_trigger = btc.get("bull_trigger_price")
    bear_trigger = btc.get("bear_trigger_price")

    reasons: List[str] = []
    if bias == "long":
        if long_ready:
            reasons.append("long_retest_ready")
        if market_regime:
            reasons.append(f"regime:{market_regime}")
        if trend_1h == "up":
            reasons.append("1h_uptrend")
        if btc.get("wall_pressure_side") == "bid":
            reasons.append("bid_wall_support")
        if btc.get("retest_long_score") is not None:
            reasons.append(f"long_retest_score:{fmt_num(btc.get('retest_long_score'))}")
    elif bias == "short":
        if short_ready:
            reasons.append("short_retest_ready")
        if market_regime:
            reasons.append(f"regime:{market_regime}")
        if trend_1h == "down":
            reasons.append("1h_downtrend")
        if btc.get("wall_pressure_side") == "ask":
            reasons.append("ask_wall_pressure")
        if btc.get("retest_short_score") is not None:
            reasons.append(f"short_retest_score:{fmt_num(btc.get('retest_short_score'))}")
    else:
        if market_regime:
            reasons.append(f"regime:{market_regime}")
        reasons.append("mixed_or_unclear_flow")

    confidence_candidates = [
        btc.get("trade_plan_confidence"),
        btc.get("confidence_score"),
        btc.get("confidence_score_v2"),
    ]
    confidence = next((round(as_float(x), 2) for x in confidence_candidates if as_float(x) is not None), None)

    verdict = "neutral / watch"
    if bias == "long":
        verdict = "long oldal előnyben a következő ~15 percre"
        if not long_ready:
            verdict = "enyhe long előny, de még nincs kész trigger"
    elif bias == "short":
        verdict = "short oldal előnyben a következő ~15 percre"
        if not short_ready:
            verdict = "enyhe short előny, de még nincs kész trigger"

    return {
        "generated_at_utc": utc_now_iso(),
        "source_ts_bucharest": ts,
        "price": last,
        "next_15m_bias": bias,
        "next_15m_confidence": confidence,
        "next_15m_verdict": verdict,
        "market_regime": market_regime,
        "market_bias": market_bias,
        "market_read": market_read,
        "summary_status": summary_status,
        "final_action": final_action,
        "trends": {
            "trend_5m": trend_5m,
            "trend_15m": trend_15m,
            "trend_1h": trend_1h,
        },
        "long": {
            "ready": long_ready,
            "entry_zone": long_entry_zone,
            "entry_trigger": bull_trigger,
            "sl": long_sl,
            "tp1": long_tp1,
            "tp2": long_tp2,
        },
        "short": {
            "ready": short_ready,
            "entry_zone": short_entry_zone,
            "entry_trigger": bear_trigger,
            "sl": short_sl,
            "tp1": short_tp1,
            "tp2": short_tp2,
        },
        "key_levels": {
            "bull_trigger": bull_trigger,
            "bear_trigger": bear_trigger,
            "invalidation_long": btc.get("invalidation_long"),
            "invalidation_short": btc.get("invalidation_short"),
            "vwap_1h": btc.get("vwap_1h"),
            "vwap_24h": btc.get("vwap_24h"),
            "session_high": btc.get("session_high"),
            "session_low": btc.get("session_low"),
            "liq_above_1": btc.get("liq_above_1"),
            "liq_below_1": btc.get("liq_below_1"),
        },
        "reasons": reasons[:5],
    }


# -----------------------------
# Rendering helpers
# -----------------------------
def endpoint_links(extra: Optional[List[str]] = None) -> str:
    links = [
        "/snapshot",
        "/snapshot-pretty",
        "/snapshot-view",
        "/next15",
        "/next15-pretty",
        "/next15-view",
        "/upload",
    ]
    if extra:
        links.extend(extra)
    rendered = "\n".join(f'<a href="{html.escape(link)}">{html.escape(link)}</a>' for link in links)
    return rendered


def make_html_page(title: str, subtitle: str, body: str) -> str:
    return f"""<!doctype html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0b1220;
      --card: #111827;
      --text: #e5e7eb;
      --muted: #9ca3af;
      --border: #1f2937;
      --accent: #60a5fa;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, monospace;
      line-height: 1.45;
    }}
    .wrap {{ max-width: 1280px; margin: 0 auto; padding: 20px; }}
    .head {{ margin-bottom: 16px; }}
    .title {{
      font-size: 22px; font-weight: 700; margin: 0 0 6px;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }}
    .sub {{
      color: var(--muted); font-size: 14px;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      margin-bottom: 12px;
    }}
    .links {{
      display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 18px;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .panel {{
      background: var(--card); border: 1px solid var(--border); border-radius: 14px;
      overflow: auto; box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
    }}
    pre {{ margin: 0; padding: 18px; font-size: 13px; white-space: pre-wrap; word-break: break-word; }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"head\">
      <h1 class=\"title\">{html.escape(title)}</h1>
      <div class=\"sub\">{html.escape(subtitle)}</div>
      <div class=\"links\">{endpoint_links()}</div>
    </div>
    <div class=\"panel\"><pre>{body}</pre></div>
  </div>
</body>
</html>"""


def zone_text(zone: Any) -> str:
    if isinstance(zone, list) and len(zone) >= 2:
        return f"{fmt_num(zone[0])} - {fmt_num(zone[1])}"
    return "-"


def next15_plain_text(report: Dict[str, Any]) -> str:
    return (
        f"Idő: {report.get('source_ts_bucharest', '-')}\n"
        f"Ár: {fmt_num(report.get('price'))}\n"
        f"Következő 15 perc: {report.get('next_15m_bias', 'neutral')}\n"
        f"Bizalom: {report.get('next_15m_confidence', '-')}\n"
        f"Verdict: {report.get('next_15m_verdict', '-')}\n\n"
        f"Long:\n"
        f"  Ready: {report.get('long', {}).get('ready', False)}\n"
        f"  Entry zone: {zone_text(report.get('long', {}).get('entry_zone'))}\n"
        f"  Trigger: {fmt_num(report.get('long', {}).get('entry_trigger'))}\n"
        f"  SL: {fmt_num(report.get('long', {}).get('sl'))}\n"
        f"  TP1: {fmt_num(report.get('long', {}).get('tp1'))}\n"
        f"  TP2: {fmt_num(report.get('long', {}).get('tp2'))}\n\n"
        f"Short:\n"
        f"  Ready: {report.get('short', {}).get('ready', False)}\n"
        f"  Entry zone: {zone_text(report.get('short', {}).get('entry_zone'))}\n"
        f"  Trigger: {fmt_num(report.get('short', {}).get('entry_trigger'))}\n"
        f"  SL: {fmt_num(report.get('short', {}).get('sl'))}\n"
        f"  TP1: {fmt_num(report.get('short', {}).get('tp1'))}\n"
        f"  TP2: {fmt_num(report.get('short', {}).get('tp2'))}\n\n"
        f"Kulcsszintek:\n"
        f"  Bull trigger: {fmt_num(report.get('key_levels', {}).get('bull_trigger'))}\n"
        f"  Bear trigger: {fmt_num(report.get('key_levels', {}).get('bear_trigger'))}\n"
        f"  1h VWAP: {fmt_num(report.get('key_levels', {}).get('vwap_1h'))}\n"
        f"  24h VWAP: {fmt_num(report.get('key_levels', {}).get('vwap_24h'))}\n"
        f"  Session high: {fmt_num(report.get('key_levels', {}).get('session_high'))}\n"
        f"  Session low: {fmt_num(report.get('key_levels', {}).get('session_low'))}\n\n"
        f"Okok: {', '.join(report.get('reasons', [])) if report.get('reasons') else '-'}\n"
    )


# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def home() -> Response:
    payload = {
        "ok": True,
        "message": "snapshot server running",
        "endpoints": {
            "snapshot": "/snapshot",
            "snapshot_pretty": "/snapshot-pretty",
            "snapshot_view": "/snapshot-view",
            "next15": "/next15",
            "next15_pretty": "/next15-pretty",
            "next15_view": "/next15-view",
            "upload": "/upload",
        },
    }
    return jsonify(payload)


@app.route("/health")
def health() -> Response:
    exists = SNAPSHOT_FILE.exists()
    payload = {
        "ok": True,
        "snapshot_exists": exists,
        "snapshot_file": str(SNAPSHOT_FILE),
        "checked_at_utc": utc_now_iso(),
    }
    if exists:
        try:
            snap = load_snapshot()
            payload["snapshot_ts_bucharest"] = snap.get("ts_bucharest")
        except Exception as exc:  # pragma: no cover - defensive
            payload["snapshot_read_error"] = str(exc)
    return jsonify(payload)


@app.route("/snapshot", methods=["GET"])
def get_snapshot() -> Response:
    if not SNAPSHOT_FILE.exists():
        return jsonify({"ok": False, "error": "snapshot not uploaded yet"}), 404
    try:
        return jsonify(load_snapshot())
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/snapshot-pretty", methods=["GET"])
def get_snapshot_pretty() -> Response:
    if not SNAPSHOT_FILE.exists():
        return Response(
            pretty_json_text({"ok": False, "error": "snapshot not uploaded yet"}),
            status=404,
            mimetype="text/plain; charset=utf-8",
        )
    try:
        return Response(pretty_json_text(load_snapshot()), mimetype="text/plain; charset=utf-8")
    except Exception as exc:
        return Response(
            pretty_json_text({"ok": False, "error": str(exc)}),
            status=500,
            mimetype="text/plain; charset=utf-8",
        )


@app.route("/snapshot-view", methods=["GET"])
def get_snapshot_view() -> Response:
    if not SNAPSHOT_FILE.exists():
        return Response(
            make_html_page("BTC Pro Snapshot", "Még nincs feltöltött snapshot.", html.escape("snapshot not uploaded yet")),
            status=404,
            mimetype="text/html; charset=utf-8",
        )
    try:
        body = html.escape(pretty_json_text(load_snapshot()))
        return Response(
            make_html_page(
                "BTC Pro Snapshot",
                "Ugyanaz a snapshot adat, csak olvashatóbb, szépen formázott nézetben.",
                body,
            ),
            mimetype="text/html; charset=utf-8",
        )
    except Exception as exc:
        return Response(
            make_html_page("BTC Pro Snapshot - Error", "Hiba történt a snapshot olvasásakor.", html.escape(str(exc))),
            status=500,
            mimetype="text/html; charset=utf-8",
        )


@app.route("/next15", methods=["GET"])
def get_next15() -> Response:
    if not SNAPSHOT_FILE.exists():
        return jsonify({"ok": False, "error": "snapshot not uploaded yet"}), 404
    try:
        report = build_next15_report(load_snapshot())
        return jsonify(report)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/next15-pretty", methods=["GET"])
def get_next15_pretty() -> Response:
    if not SNAPSHOT_FILE.exists():
        return Response(
            pretty_json_text({"ok": False, "error": "snapshot not uploaded yet"}),
            status=404,
            mimetype="text/plain; charset=utf-8",
        )
    try:
        report = build_next15_report(load_snapshot())
        return Response(next15_plain_text(report), mimetype="text/plain; charset=utf-8")
    except Exception as exc:
        return Response(
            pretty_json_text({"ok": False, "error": str(exc)}),
            status=500,
            mimetype="text/plain; charset=utf-8",
        )


@app.route("/next15-view", methods=["GET"])
def get_next15_view() -> Response:
    if not SNAPSHOT_FILE.exists():
        return Response(
            make_html_page("BTC Pro Next 15m", "Még nincs feltöltött snapshot.", html.escape("snapshot not uploaded yet")),
            status=404,
            mimetype="text/html; charset=utf-8",
        )
    try:
        report = build_next15_report(load_snapshot())
        body = html.escape(next15_plain_text(report))
        return Response(
            make_html_page(
                "BTC Pro Next 15m",
                "Rövid, tömör 15 perces bias jelentés long és short setup szintekkel.",
                body,
            ),
            mimetype="text/html; charset=utf-8",
        )
    except Exception as exc:
        return Response(
            make_html_page("BTC Pro Next 15m - Error", "Hiba történt a riport készítésekor.", html.escape(str(exc))),
            status=500,
            mimetype="text/html; charset=utf-8",
        )


@app.route("/upload", methods=["POST"])
def upload_snapshot() -> Response:
    try:
        data = request.get_json(force=True)
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "JSON object required"}), 400
        write_snapshot(data)
        return jsonify({
            "ok": True,
            "snapshot_ts_bucharest": data.get("ts_bucharest"),
            "received_at_utc": utc_now_iso(),
        })
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
