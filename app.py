from __future__ import annotations

from datetime import datetime, timezone
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


def _json_response(payload: Any, status: int = 200):
    resp = make_response(jsonify(payload), status)
    return _apply_no_cache(resp)


def _text_response(text: str, status: int = 200, content_type: str = "text/plain; charset=utf-8"):
    resp = make_response(text, status)
    resp.headers["Content-Type"] = content_type
    return _apply_no_cache(resp)


def _apply_no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.after_request
def add_no_cache_headers(resp):
    return _apply_no_cache(resp)


def _fmt_num(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, (int, float)):
        if abs(v) >= 1000:
            return f"{v:,.2f}".replace(",", " ")
        return f"{v:.2f}"
    return str(v)


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


def _pretty_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _html_page(title: str, payload: Any) -> str:
    bust = request.args.get("ts", "")
    return f"""<!doctype html>
<html lang='hu'>
<head>
<meta charset='utf-8'>
<meta http-equiv='Cache-Control' content='no-store, no-cache, must-revalidate, max-age=0'>
<meta http-equiv='Pragma' content='no-cache'>
<meta http-equiv='Expires' content='0'>
<title>{title}</title>
<style>
body{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;background:#111;color:#eee;padding:24px}}
pre{{white-space:pre-wrap;word-break:break-word;background:#1b1b1b;padding:16px;border-radius:10px}}
.small{{opacity:.7;margin-bottom:12px}}
</style>
</head>
<body>
<h1>{title}</h1>
<div class='small'>cache-bust ts: {bust or '-'}</div>
<pre>{_pretty_json(payload)}</pre>
</body>
</html>"""


@app.route("/")
def home():
    return _json_response(
        {
            "ok": True,
            "message": "snapshot server running",
            "endpoints": [
                "/snapshot",
                "/snapshot-pretty",
                "/snapshot-view",
                "/trade",
                "/trade-pretty",
                "/trade-view",
                "/next15",
                "/next15-pretty",
                "/next15-view",
                "/upload",
            ],
        }
    )


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
        return _text_response(_html_page("Snapshot View", _load_snapshot()), content_type="text/html; charset=utf-8")
    except FileNotFoundError as e:
        return _text_response(_html_page("Snapshot View", {"ok": False, "error": str(e)}), 404, "text/html; charset=utf-8")
    except Exception as e:
        return _text_response(_html_page("Snapshot View", {"ok": False, "error": str(e)}), 500, "text/html; charset=utf-8")


@app.route("/trade", methods=["GET"])
@app.route("/next15", methods=["GET"])
def get_trade_json():
    try:
        data = _load_snapshot()
        if request.path.endswith("next15"):
            return _json_response(_extract_next15(data))
        return _json_response(_extract_trade_view(data))
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
        title = "Next15 View" if request.path.endswith("next15-view") else "Trade View"
        payload = _extract_next15(data) if request.path.endswith("next15-view") else _extract_trade_view(data)
        return _text_response(_html_page(title, payload), content_type="text/html; charset=utf-8")
    except FileNotFoundError as e:
        return _text_response(_html_page("Trade View", {"ok": False, "error": str(e)}), 404, "text/html; charset=utf-8")
    except Exception as e:
        return _text_response(_html_page("Trade View", {"ok": False, "error": str(e)}), 500, "text/html; charset=utf-8")


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
    app.run(host="0.0.0.0", port=10000, debug=False)
