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


def fmt_num(value: Any, digits: int = 2) -> str:
    num = as_float(value)
    if num is None:
        return "-"
    return f"{num:.{digits}f}"


def zone_text(zone: Any) -> str:
    if isinstance(zone, list) and len(zone) >= 2:
        return f"{fmt_num(zone[0])} - {fmt_num(zone[1])}"
    return "-"


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def to_bool(value: Any) -> bool:
    return bool(value)


# -----------------------------
# Next-15 report logic
# -----------------------------
def _dominant_bias_from_htf(btc: Dict[str, Any]) -> str:
    trend_15m = btc.get("trend_15m")
    trend_1h = btc.get("trend_1h")
    trend_4h = btc.get("htf_4h_trend")
    market_bias = btc.get("market_bias")
    market_regime = btc.get("market_regime") or ""

    up_votes = 0
    down_votes = 0

    if trend_1h == "up":
        up_votes += 3
    elif trend_1h == "down":
        down_votes += 3

    if trend_15m == "up":
        up_votes += 2
    elif trend_15m == "down":
        down_votes += 2
    elif trend_15m == "range":
        if trend_1h == "up":
            up_votes += 1
        elif trend_1h == "down":
            down_votes += 1

    if trend_4h == "up":
        up_votes += 2
    elif trend_4h == "down":
        down_votes += 2

    if market_bias == "long":
        up_votes += 1
    elif market_bias == "short":
        down_votes += 1

    if "long" in market_regime or "impulse_up" in market_regime or "trend_build_long" in market_regime:
        up_votes += 1
    if "short" in market_regime or "impulse_down" in market_regime or "trend_build_short" in market_regime:
        down_votes += 1

    if up_votes >= down_votes + 2:
        return "long"
    if down_votes >= up_votes + 2:
        return "short"
    return "neutral"


def _execution_bias_from_ltf(btc: Dict[str, Any]) -> str:
    final_side = btc.get("final_side_v4") or btc.get("final_side_v3") or btc.get("final_side_v2") or btc.get("final_side")
    retest_winner = btc.get("retest_winner_side")
    trend_5m = btc.get("trend_5m")
    raw_bias = btc.get("trade_bias") or btc.get("trading_bias")

    long_score = 0
    short_score = 0

    if final_side == "long":
        long_score += 3
    elif final_side == "short":
        short_score += 3

    if retest_winner == "long":
        long_score += 2
    elif retest_winner == "short":
        short_score += 2

    if trend_5m == "up":
        long_score += 1
    elif trend_5m == "down":
        short_score += 1

    if raw_bias == "long":
        long_score += 1
    elif raw_bias == "short":
        short_score += 1

    if long_score >= short_score + 2:
        return "long"
    if short_score >= long_score + 2:
        return "short"
    return "neutral"


def _bias_stability_score(btc: Dict[str, Any], dominant_bias: str, execution_bias: str) -> float:
    score = 40.0
    persistence = as_float(btc.get("bias_persistence_count")) or 0.0
    regime_persistence = as_float(btc.get("regime_persistence_count")) or 0.0
    stability = as_float(btc.get("decision_stability_score")) or 0.0
    conflict = as_float(btc.get("signal_conflict_score")) or 0.0

    score += min(persistence * 8.0, 24.0)
    score += min(regime_persistence * 4.0, 12.0)
    score += min(stability * 0.22, 20.0)
    score -= min(conflict * 0.30, 20.0)

    if dominant_bias != "neutral" and dominant_bias == execution_bias:
        score += 10.0
    elif dominant_bias != "neutral" and execution_bias not in ("neutral", dominant_bias):
        score -= 12.0

    return round(max(0.0, min(100.0, score)), 2)


def _next15_bias(btc: Dict[str, Any]) -> Dict[str, Any]:
    dominant_bias = _dominant_bias_from_htf(btc)
    execution_bias = _execution_bias_from_ltf(btc)
    prev_bias = btc.get("prev_trade_bias")
    bias_persistence = int(as_float(btc.get("bias_persistence_count")) or 0)

    long_ready = to_bool(btc.get("retest_long_ready") or btc.get("long_path_valid"))
    short_ready = to_bool(btc.get("retest_short_ready") or btc.get("short_path_valid"))
    long_score = as_float(btc.get("retest_long_score")) or 0.0
    short_score = as_float(btc.get("retest_short_score")) or 0.0
    long_consensus = as_float(btc.get("direction_consensus_long_score")) or 0.0
    short_consensus = as_float(btc.get("direction_consensus_short_score")) or 0.0

    if dominant_bias == "long":
        if short_ready and execution_bias == "short" and short_score >= long_score + 18 and short_consensus >= long_consensus + 15 and bias_persistence >= 2:
            final_bias = "short"
        else:
            final_bias = "long"
    elif dominant_bias == "short":
        if long_ready and execution_bias == "long" and long_score >= short_score + 18 and long_consensus >= short_consensus + 15 and bias_persistence >= 2:
            final_bias = "long"
        else:
            final_bias = "short"
    else:
        if execution_bias in ("long", "short"):
            final_bias = execution_bias
        elif long_ready and long_score >= short_score + 12:
            final_bias = "long"
        elif short_ready and short_score >= long_score + 12:
            final_bias = "short"
        else:
            final_bias = "neutral"

    flip_cooldown_active = False
    if prev_bias in ("long", "short") and final_bias in ("long", "short") and prev_bias != final_bias:
        if bias_persistence < 2 and dominant_bias not in ("neutral", final_bias):
            final_bias = prev_bias
            flip_cooldown_active = True

    stability = _bias_stability_score(btc, dominant_bias, execution_bias)
    flip_risk = round(max(0.0, min(100.0, 100.0 - stability)), 2)

    return {
        "dominant_bias_1h_15m": dominant_bias,
        "execution_bias_5m": execution_bias,
        "next_15m_bias": final_bias,
        "bias_stability": stability,
        "bias_flip_risk": flip_risk,
        "flip_cooldown_active": flip_cooldown_active,
    }


def _bias_confidence(btc: Dict[str, Any], bias_meta: Dict[str, Any]) -> Optional[float]:
    dominant = bias_meta.get("dominant_bias_1h_15m")
    execution = bias_meta.get("execution_bias_5m")
    final_bias = bias_meta.get("next_15m_bias")

    score = 0.0
    if final_bias == "neutral":
        score = 42.0
    else:
        score = 50.0
        if dominant == final_bias:
            score += 12.0
        if execution == final_bias:
            score += 8.0

        if final_bias == "long":
            score += min((as_float(btc.get("retest_long_score")) or 0.0) * 0.16, 14.0)
            score += min((as_float(btc.get("direction_consensus_long_score")) or 0.0) * 0.10, 10.0)
        elif final_bias == "short":
            score += min((as_float(btc.get("retest_short_score")) or 0.0) * 0.16, 14.0)
            score += min((as_float(btc.get("direction_consensus_short_score")) or 0.0) * 0.10, 10.0)

        score += min((as_float(btc.get("volume_quality_score")) or 0.0) * 0.10, 8.0)
        score += min((as_float(bias_meta.get("bias_stability")) or 0.0) * 0.10, 10.0)

        if bias_meta.get("flip_cooldown_active"):
            score -= 8.0

        if btc.get("signal_conflict_score") is not None:
            score -= min((as_float(btc.get("signal_conflict_score")) or 0.0) * 0.18, 14.0)

    return round(max(0.0, min(100.0, score)), 2)


def _verdict_and_reasons(btc: Dict[str, Any], bias_meta: Dict[str, Any]) -> Dict[str, Any]:
    final_bias = bias_meta.get("next_15m_bias")
    dominant = bias_meta.get("dominant_bias_1h_15m")
    execution = bias_meta.get("execution_bias_5m")
    reasons: List[str] = []

    market_regime = btc.get("market_regime")
    if market_regime:
        reasons.append(f"regime:{market_regime}")

    if dominant == "long":
        reasons.append("1h_15m_főirány:long")
    elif dominant == "short":
        reasons.append("1h_15m_főirány:short")
    else:
        reasons.append("1h_15m_főirány:semleges")

    if execution == "long":
        reasons.append("5m_execution:long")
    elif execution == "short":
        reasons.append("5m_execution:short")
    else:
        reasons.append("5m_execution:semleges")

    if final_bias == "long":
        if btc.get("retest_long_ready"):
            reasons.append("long_retest_ready")
        if btc.get("trend_1h") == "up":
            reasons.append("1h_trend_up")
        if btc.get("wall_pressure_side") == "bid":
            reasons.append("bid_támasz")
    elif final_bias == "short":
        if btc.get("retest_short_ready"):
            reasons.append("short_retest_ready")
        if btc.get("trend_1h") == "down":
            reasons.append("1h_trend_down")
        if btc.get("wall_pressure_side") == "ask":
            reasons.append("ask_nyomás")

    if bias_meta.get("flip_cooldown_active"):
        reasons.append("flip_cooldown")

    verdict = "semleges / kivárás"
    if final_bias == "long":
        if dominant == "long" and execution in ("neutral", "long"):
            verdict = "főirány long, a következő 15 percben is inkább long előny"
        elif dominant == "long" and execution == "short":
            verdict = "HTF long, most inkább visszahúzás; nem teljes irányváltás"
        else:
            verdict = "enyhe long előny, de még kell megerősítés"
    elif final_bias == "short":
        if dominant == "short" and execution in ("neutral", "short"):
            verdict = "főirány short, a következő 15 percben is inkább short előny"
        elif dominant == "short" and execution == "long":
            verdict = "HTF short, most inkább felpattanás; nem teljes irányváltás"
        else:
            verdict = "enyhe short előny, de még kell megerősítés"

    return {
        "next_15m_verdict": verdict,
        "reasons": reasons[:6],
    }


def build_next15_report(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    btc = snapshot.get("btc", {}) if isinstance(snapshot.get("btc"), dict) else {}
    trade_report = btc.get("trade_report", {}) if isinstance(btc.get("trade_report"), dict) else {}

    bias_meta = _next15_bias(btc)
    verdict_meta = _verdict_and_reasons(btc, bias_meta)
    confidence = _bias_confidence(btc, bias_meta)

    long_entry_zone = first_not_none(
        trade_report.get("long_entry_zone"),
        btc.get("trade_plan_entry_zone") if btc.get("trade_plan_side") == "long" else None,
        btc.get("long_entry_zone"),
    )
    short_entry_zone = first_not_none(
        trade_report.get("short_entry_zone"),
        btc.get("trade_plan_entry_zone") if btc.get("trade_plan_side") == "short" else None,
        btc.get("short_entry_zone"),
    )

    long_sl = first_not_none(
        trade_report.get("long_sl"),
        btc.get("trade_plan_stop") if btc.get("trade_plan_side") == "long" else None,
        btc.get("atr_stop_long"),
        btc.get("invalidation_long"),
    )
    short_sl = first_not_none(
        trade_report.get("short_sl"),
        btc.get("trade_plan_stop") if btc.get("trade_plan_side") == "short" else None,
        btc.get("atr_stop_short"),
        btc.get("invalidation_short"),
    )

    long_tp1 = first_not_none(trade_report.get("long_tp1"), btc.get("trade_plan_t1"), btc.get("target_long_1"))
    long_tp2 = first_not_none(trade_report.get("long_tp2"), btc.get("trade_plan_t2"), btc.get("target_long_2"))
    short_tp1 = first_not_none(trade_report.get("short_tp1"), btc.get("target_short_1"))
    short_tp2 = first_not_none(trade_report.get("short_tp2"), btc.get("target_short_2"))

    long_ready = to_bool(btc.get("retest_long_ready") or btc.get("long_path_valid") or btc.get("trade_plan_side") == "long")
    short_ready = to_bool(btc.get("retest_short_ready") or btc.get("short_path_valid") or btc.get("trade_plan_side") == "short")

    return {
        "generated_at_utc": utc_now_iso(),
        "source_ts_bucharest": snapshot.get("ts_bucharest"),
        "price": btc.get("last"),
        "next_15m_bias": bias_meta.get("next_15m_bias"),
        "next_15m_confidence": confidence,
        "next_15m_verdict": verdict_meta.get("next_15m_verdict"),
        "dominant_bias_1h_15m": bias_meta.get("dominant_bias_1h_15m"),
        "execution_bias_5m": bias_meta.get("execution_bias_5m"),
        "bias_stability": bias_meta.get("bias_stability"),
        "bias_flip_risk": bias_meta.get("bias_flip_risk"),
        "flip_cooldown_active": bias_meta.get("flip_cooldown_active"),
        "market_regime": btc.get("market_regime"),
        "market_bias": btc.get("market_bias"),
        "market_read": btc.get("market_read"),
        "summary_status": btc.get("summary_status"),
        "final_action": first_not_none(btc.get("final_action_v4"), btc.get("final_action_v3"), btc.get("final_action_v2"), btc.get("final_action")),
        "trends": {
            "trend_5m": btc.get("trend_5m"),
            "trend_15m": btc.get("trend_15m"),
            "trend_1h": btc.get("trend_1h"),
            "trend_4h": btc.get("htf_4h_trend"),
        },
        "long": {
            "ready": long_ready,
            "entry_zone": long_entry_zone,
            "entry_trigger": btc.get("bull_trigger_price"),
            "sl": long_sl,
            "tp1": long_tp1,
            "tp2": long_tp2,
        },
        "short": {
            "ready": short_ready,
            "entry_zone": short_entry_zone,
            "entry_trigger": btc.get("bear_trigger_price"),
            "sl": short_sl,
            "tp1": short_tp1,
            "tp2": short_tp2,
        },
        "key_levels": {
            "bull_trigger": btc.get("bull_trigger_price"),
            "bear_trigger": btc.get("bear_trigger_price"),
            "invalidation_long": btc.get("invalidation_long"),
            "invalidation_short": btc.get("invalidation_short"),
            "vwap_1h": btc.get("vwap_1h"),
            "vwap_24h": btc.get("vwap_24h"),
            "session_high": btc.get("session_high"),
            "session_low": btc.get("session_low"),
            "liq_above_1": btc.get("liq_above_1"),
            "liq_below_1": btc.get("liq_below_1"),
        },
        "reasons": verdict_meta.get("reasons", []),
    }


# -----------------------------
# Rendering helpers
# -----------------------------
def endpoint_links() -> str:
    links = [
        "/snapshot",
        "/snapshot-pretty",
        "/snapshot-view",
        "/next15",
        "/next15-pretty",
        "/next15-view",
        "/upload",
    ]
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


def next15_plain_text(report: Dict[str, Any]) -> str:
    return (
        f"Idő: {report.get('source_ts_bucharest', '-')}\n"
        f"Ár: {fmt_num(report.get('price'))}\n"
        f"Következő 15 perc: {report.get('next_15m_bias', 'neutral')}\n"
        f"Bizalom: {fmt_num(report.get('next_15m_confidence'))}\n"
        f"Verdict: {report.get('next_15m_verdict', '-')}\n"
        f"Főirány (1h/15m): {report.get('dominant_bias_1h_15m', '-')}\n"
        f"Execution bias (5m): {report.get('execution_bias_5m', '-')}\n"
        f"Bias stabilitás: {fmt_num(report.get('bias_stability'))}\n"
        f"Bias flip risk: {fmt_num(report.get('bias_flip_risk'))}\n"
        f"Flip cooldown aktív: {report.get('flip_cooldown_active', False)}\n\n"
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
        except Exception as exc:
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
                "Rövid 15 perces bias jelentés, erősebb 1h/15m szűrővel és lassabb irányváltással.",
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
