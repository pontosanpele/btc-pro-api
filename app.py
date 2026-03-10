from flask import Flask, jsonify, request, Response
from pathlib import Path
import json
import html

app = Flask(__name__)
SNAPSHOT_FILE = Path("snapshot.json")


def _load_snapshot():
    if not SNAPSHOT_FILE.exists():
        return None, (jsonify({"ok": False, "error": "snapshot not uploaded yet"}), 404)
    try:
        with SNAPSHOT_FILE.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except Exception as e:
        return None, (jsonify({"ok": False, "error": str(e)}), 500)


def _no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    resp.headers["Surrogate-Control"] = "no-store"
    resp.headers["Vary"] = "Accept, Accept-Encoding"
    return resp


@app.after_request
def add_cache_busting_headers(resp):
    return _no_cache(resp)


@app.route("/")
def home():
    _ = request.args.get("ts") or request.args.get("v") or request.args.get("cb")
    return {
        "ok": True,
        "message": "snapshot server running",
        "cache_busting": "append ?ts=<unix> to any GET endpoint"
    }


@app.route("/snapshot", methods=["GET"])
def get_snapshot():
    _ = request.args.get("ts") or request.args.get("v") or request.args.get("cb")
    data, err = _load_snapshot()
    if err:
        return err
    return jsonify(data)


@app.route("/snapshot-pretty", methods=["GET"])
def get_snapshot_pretty():
    _ = request.args.get("ts") or request.args.get("v") or request.args.get("cb")
    data, err = _load_snapshot()
    if err:
        return err
    return Response(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False),
        mimetype="text/plain; charset=utf-8",
    )


@app.route("/snapshot-view", methods=["GET"])
def get_snapshot_view():
    bust = request.args.get("ts") or request.args.get("v") or request.args.get("cb") or ""
    data, err = _load_snapshot()
    if err:
        return err
    pretty = html.escape(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False))
    page = f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta http-equiv='Cache-Control' content='no-store, no-cache, must-revalidate, max-age=0'>
  <meta http-equiv='Pragma' content='no-cache'>
  <meta http-equiv='Expires' content='0'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Snapshot View</title>
  <style>
    body {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin: 24px; background:#0b1020; color:#e6edf3; }}
    .meta {{ margin-bottom: 12px; color:#9fb0c3; }}
    pre {{ white-space: pre-wrap; word-break: break-word; background:#11182b; padding:16px; border-radius:12px; }}
    a {{ color:#7cc7ff; }}
  </style>
</head>
<body>
  <div class='meta'>cache-bust={html.escape(str(bust)) or 'none'} | tip: add <code>?ts=UNIXTIME</code></div>
  <pre>{pretty}</pre>
</body>
</html>"""
    return Response(page, mimetype="text/html; charset=utf-8")


@app.route("/upload", methods=["POST"])
def upload_snapshot():
    try:
        data = request.get_json(force=True)
        with SNAPSHOT_FILE.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
