from flask import Flask, jsonify, request, Response
from pathlib import Path
import json, html, math
from datetime import datetime, timezone

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


def _cache_bust_value():
    return request.args.get("ts") or request.args.get("v") or request.args.get("cb") or ""


def _load_snapshot():
    if not SNAPSHOT_FILE.exists():
        return None, (jsonify({"ok": False, "error": "snapshot not uploaded yet"}), 404)
    try:
        with SNAPSHOT_FILE.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except Exception as e:
        return None, (jsonify({"ok": False, "error": str(e)}), 500)


def _round(v, n=2):
    try:
        return round(float(v), n)
    except Exception:
        return None


def _pick(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _entry_zone(d, side):
    zone = _pick(d, "btc", f"{side}_entry_zone") or _pick(d, "btc", "trade_report", f"{side}_entry_zone")
    if isinstance(zone, (list, tuple)) and len(zone) >= 2:
        return [_round(zone[0]), _round(zone[1])]
    trigger = _pick(d, "btc", f"{'bull' if side=='long' else 'bear'}_trigger_price")
    atr = _pick(d, "btc", "atr_5m") or 0
    if trigger is None:
        return None
    half = max(float(atr) * 0.08, float(trigger) * 0.00025)
    return [_round(float(trigger) - half), _round(float(trigger) + half)]


def _side_block(d, side):
    b = _pick(d, "btc", {})
    report = b.get("trade_report", {}) if isinstance(b, dict) else {}
    entry = _entry_zone(d, side)
    data = {
        "entry_zone": entry,
        "sl": _round(report.get(f"{side}_sl", b.get(f"atr_stop_{side}"))),
        "tp1": _round(report.get(f"{side}_tp1", b.get(f"target_{side}_1"))),
        "tp2": _round(report.get(f"{side}_tp2", b.get(f"target_{side}_2"))),
        "ready": bool(b.get(f"retest_{side}_ready", False)),
        "score": _round(b.get(f"retest_{side}_score", 0)),
    }
    return data


def _bias_verdict(d):
    b = _pick(d, "btc", {})
    final_side = b.get("final_side_v4") or b.get("final_side") or b.get("trade_bias") or "neutral"
    dominant = b.get("dominant_bias_htf") or final_side or "neutral"
    execution = b.get("execution_bias_ltf") or ("long" if b.get("retest_long_ready") else "short" if b.get("retest_short_ready") else "neutral")
    rr_l = float(b.get("rr_long_to_t1") or 0)
    rr_s = float(b.get("rr_short_to_t1") or 0)
    if final_side == "long" and rr_l < 1.0:
        verdict = "LATE_LONG"
    elif final_side == "short" and rr_s < 1.0:
        verdict = "LATE_SHORT"
    elif final_side == "long" and not b.get("retest_long_ready"):
        verdict = "LONG_CONFIRM_WAIT"
    elif final_side == "short" and not b.get("retest_short_ready"):
        verdict = "SHORT_CONFIRM_WAIT"
    else:
        verdict = (b.get("final_action_v4") or b.get("final_action") or b.get("trade_report", {}).get("verdict") or str(final_side).upper())
    return dominant, execution, verdict


def _confidence(d):
    b = _pick(d, "btc", {})
    return {
        "direction": _round(b.get("confidence_direction", b.get("confidence_score", 0))),
        "execution": _round(b.get("confidence_execution", b.get("execution_feasibility_score", 0))),
        "rr": _round(b.get("confidence_rr", max(float(b.get("rr_long_to_t1") or 0), float(b.get("rr_short_to_t1") or 0)) * 50)),
        "external": _round(b.get("confidence_external", b.get("external_confirmation_score", 0))),
    }


def _next15_summary(d):
    b = _pick(d, "btc", {})
    dominant, execution, verdict = _bias_verdict(d)
    long_block = _side_block(d, "long")
    short_block = _side_block(d, "short")
    bull = _round(b.get("bull_trigger_price"))
    bear = _round(b.get("bear_trigger_price"))
    ts = d.get("ts_bucharest")
    price = _round(b.get("last"))
    reasons = []
    if b.get("retest_winner_side"):
        reasons.append(f"retest winner: {b.get('retest_winner_side')}")
    if b.get("market_regime"):
        reasons.append(f"regime: {b.get('market_regime')}")
    if b.get("trend_1h"):
        reasons.append(f"1h trend: {b.get('trend_1h')}")
    if b.get("wall_pressure_side"):
        reasons.append(f"wall pressure: {b.get('wall_pressure_side')}")
    if b.get("external_confirmation_alignment"):
        reasons.append(f"external: {b.get('external_confirmation_alignment')}")
    reasons = reasons[:4]
    conf = _confidence(d)
    overall = _round((float(conf["direction"] or 0) * 0.4 + float(conf["execution"] or 0) * 0.3 + float(conf["rr"] or 0) * 0.2 + float(conf["external"] or 0) * 0.1), 1)
    return {
        "ts_bucharest": ts,
        "server_updated_at": d.get("server_updated_at"),
        "price": price,
        "dominant_bias_1h_15m": dominant,
        "execution_bias_5m": execution,
        "next_15m_bias": dominant if dominant != "neutral" else execution,
        "confidence": overall,
        "verdict": verdict,
        "long": long_block,
        "short": short_block,
        "key_levels": {
            "bull_trigger": bull,
            "bear_trigger": bear,
            "major_liq_above": _round(b.get("major_liq_above") or b.get("liq_above_1")),
            "major_liq_below": _round(b.get("major_liq_below") or b.get("liq_below_1")),
            "vwap_1h": _round(b.get("vwap_1h")),
            "vwap_24h": _round(b.get("vwap_24h")),
        },
        "confidence_breakdown": conf,
        "reasons": reasons,
    }


def _trade_text(rep):
    def zone(z):
        if not z:
            return "n/a"
        return f"{z[0]} – {z[1]}"
    return (
        f"Idő: {rep.get('ts_bucharest')}\n"
        f"Ár: {rep.get('price')}\n"
        f"Domináns bias: {rep.get('dominant_bias_1h_15m')}\n"
        f"Execution bias: {rep.get('execution_bias_5m')}\n"
        f"Verdict: {rep.get('verdict')}\n"
        f"Confidence: {rep.get('confidence')}\n\n"
        f"Long:\n"
        f"  Entry: {zone(rep['long'].get('entry_zone'))}\n"
        f"  SL: {rep['long'].get('sl')}\n"
        f"  TP1: {rep['long'].get('tp1')}\n"
        f"  TP2: {rep['long'].get('tp2')}\n"
        f"  Ready: {rep['long'].get('ready')} | Score: {rep['long'].get('score')}\n\n"
        f"Short:\n"
        f"  Entry: {zone(rep['short'].get('entry_zone'))}\n"
        f"  SL: {rep['short'].get('sl')}\n"
        f"  TP1: {rep['short'].get('tp1')}\n"
        f"  TP2: {rep['short'].get('tp2')}\n"
        f"  Ready: {rep['short'].get('ready')} | Score: {rep['short'].get('score')}\n\n"
        f"Kulcsszintek:\n"
        f"  Bull trigger: {rep['key_levels'].get('bull_trigger')}\n"
        f"  Bear trigger: {rep['key_levels'].get('bear_trigger')}\n"
        f"  Major liq above: {rep['key_levels'].get('major_liq_above')}\n"
        f"  Major liq below: {rep['key_levels'].get('major_liq_below')}\n"
        f"  VWAP 1h: {rep['key_levels'].get('vwap_1h')}\n"
        f"  VWAP 24h: {rep['key_levels'].get('vwap_24h')}\n\n"
        f"Miért:\n  - " + "\n  - ".join(rep.get("reasons") or ["n/a"])
    )


@app.route("/")
def home():
    bust = _cache_bust_value()
    return {
        "ok": True,
        "message": "snapshot server running",
        "cache_busting": "append ?ts=<unix> to any GET endpoint",
        "cache_bust": bust or None,
        "endpoints": [
            "/snapshot", "/snapshot-pretty", "/snapshot-view",
            "/next15", "/next15-pretty", "/next15-view",
            "/trade", "/trade-pretty", "/trade-view",
            "/upload"
        ]
    }


@app.route("/snapshot", methods=["GET"])
def get_snapshot():
    _cache_bust_value()
    data, err = _load_snapshot()
    if err:
        return err
    return jsonify(data)


@app.route("/snapshot-pretty", methods=["GET"])
def get_snapshot_pretty():
    _cache_bust_value()
    data, err = _load_snapshot()
    if err:
        return err
    return Response(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False), mimetype="text/plain; charset=utf-8")


@app.route("/snapshot-view", methods=["GET"])
def get_snapshot_view():
    bust = _cache_bust_value()
    data, err = _load_snapshot()
    if err:
        return err
    pretty = html.escape(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False))
    page = f"""<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>Snapshot View</title><style>body{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;margin:24px;background:#0b1020;color:#e6edf3}}.meta{{margin-bottom:12px;color:#9fb0c3}}pre{{white-space:pre-wrap;word-break:break-word;background:#11182b;padding:16px;border-radius:12px}}a{{color:#7cc7ff}}</style></head><body><div class='meta'>cache-bust={html.escape(str(bust)) or 'none'} | tip: add <code>?ts=UNIXTIME</code></div><pre>{pretty}</pre></body></html>"""
    return Response(page, mimetype="text/html; charset=utf-8")


@app.route("/next15", methods=["GET"])
def get_next15():
    _cache_bust_value()
    data, err = _load_snapshot()
    if err:
        return err
    return jsonify(_next15_summary(data))


@app.route("/next15-pretty", methods=["GET"])
def get_next15_pretty():
    _cache_bust_value()
    data, err = _load_snapshot()
    if err:
        return err
    rep = _next15_summary(data)
    return Response(_trade_text(rep), mimetype="text/plain; charset=utf-8")


@app.route("/next15-view", methods=["GET"])
def get_next15_view():
    bust = _cache_bust_value()
    data, err = _load_snapshot()
    if err:
        return err
    rep = _next15_summary(data)
    pretty = html.escape(_trade_text(rep))
    page = f"""<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>Next15 View</title><style>body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:24px;background:#0b1020;color:#e6edf3}}.meta{{margin-bottom:12px;color:#9fb0c3}}pre{{white-space:pre-wrap;word-break:break-word;background:#11182b;padding:16px;border-radius:12px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}}code{{background:#11182b;padding:2px 6px;border-radius:6px}}</style></head><body><div class='meta'>cache-bust={html.escape(str(bust)) or 'none'} | tip: add <code>?ts=UNIXTIME</code></div><pre>{pretty}</pre></body></html>"""
    return Response(page, mimetype="text/html; charset=utf-8")


@app.route("/trade", methods=["GET"])
def get_trade():
    return get_next15()


@app.route("/trade-pretty", methods=["GET"])
def get_trade_pretty():
    return get_next15_pretty()


@app.route("/trade-view", methods=["GET"])
def get_trade_view():
    return get_next15_view()


@app.route("/upload", methods=["POST"])
def upload_snapshot():
    try:
        data = request.get_json(force=True)
        if isinstance(data, dict):
            data["server_updated_at"] = _utc_now_iso()
        with SNAPSHOT_FILE.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return jsonify({"ok": True, "server_updated_at": data.get("server_updated_at") if isinstance(data, dict) else None})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
