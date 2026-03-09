from flask import Flask, jsonify
from btc_pro_strategy import build_snapshot

app = Flask(__name__)

@app.route("/")
def index():
    try:
        snapshot = build_snapshot()
        return jsonify(snapshot)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
