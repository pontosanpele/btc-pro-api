from flask import Flask, jsonify, request
from pathlib import Path
import json

app = Flask(__name__)
SNAPSHOT_FILE = Path("snapshot.json")

@app.route("/")
def home():
    return {"ok": True, "message": "snapshot server running"}

@app.route("/snapshot", methods=["GET"])
def get_snapshot():
    if not SNAPSHOT_FILE.exists():
        return jsonify({"ok": False, "error": "snapshot not uploaded yet"}), 404
    try:
        with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/upload", methods=["POST"])
def upload_snapshot():
    try:
        data = request.get_json(force=True)
        with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
