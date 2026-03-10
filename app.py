from flask import Flask, jsonify, request, Response
from pathlib import Path
import html
import json
from typing import Any, Dict

app = Flask(__name__)
SNAPSHOT_FILE = Path("snapshot.json")


def load_snapshot() -> Dict[str, Any]:
    if not SNAPSHOT_FILE.exists():
        raise FileNotFoundError("snapshot not uploaded yet")
    with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def pretty_json_text(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=False)


def snapshot_html_page(data: Dict[str, Any]) -> str:
    body = html.escape(pretty_json_text(data))
    return f"""<!doctype html>
<html lang=\"hu\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>BTC Pro Snapshot</title>
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
    .wrap {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 20px;
    }}
    .head {{ margin-bottom: 16px; }}
    .title {{
      font-size: 22px;
      font-weight: 700;
      margin: 0 0 6px;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    .sub {{
      color: var(--muted);
      font-size: 14px;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin-bottom: 12px;
    }}
    .links {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 18px;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .panel {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      overflow: auto;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
    }}
    pre {{
      margin: 0;
      padding: 18px;
      font-size: 13px;
      white-space: pre-wrap;
      word-break: break-word;
    }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"head\">
      <h1 class=\"title\">BTC Pro Snapshot</h1>
      <div class=\"sub\">Ugyanaz a snapshot adat, csak olvashatóbb, szépen formázott nézetben.</div>
      <div class=\"links\">
        <a href=\"/snapshot\">/snapshot</a>
        <a href=\"/snapshot-pretty\">/snapshot-pretty</a>
        <a href=\"/snapshot-view\">/snapshot-view</a>
      </div>
    </div>
    <div class=\"panel\"><pre>{body}</pre></div>
  </div>
</body>
</html>"""


@app.route("/")
def home():
    return {
        "ok": True,
        "message": "snapshot server running",
        "endpoints": {
            "snapshot": "/snapshot",
            "snapshot_pretty": "/snapshot-pretty",
            "snapshot_view": "/snapshot-view",
            "upload": "/upload",
        },
    }


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
        return Response(
            json.dumps({"ok": False, "error": "snapshot not uploaded yet"}, ensure_ascii=False, indent=2),
            status=404,
            mimetype="text/plain; charset=utf-8",
        )
    try:
        data = load_snapshot()
        return Response(pretty_json_text(data), mimetype="text/plain; charset=utf-8")
    except Exception as e:
        return Response(
            json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False, indent=2),
            status=500,
            mimetype="text/plain; charset=utf-8",
        )


@app.route("/snapshot-view", methods=["GET"])
def get_snapshot_view():
    if not SNAPSHOT_FILE.exists():
        return Response(
            "<h1>snapshot not uploaded yet</h1>",
            status=404,
            mimetype="text/html; charset=utf-8",
        )
    try:
        data = load_snapshot()
        return Response(snapshot_html_page(data), mimetype="text/html; charset=utf-8")
    except Exception as e:
        return Response(
            f"<h1>error</h1><pre>{html.escape(str(e))}</pre>",
            status=500,
            mimetype="text/html; charset=utf-8",
        )


@app.route("/upload", methods=["POST"])
def upload_snapshot():
    try:
        data = request.get_json(force=True)
        with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
