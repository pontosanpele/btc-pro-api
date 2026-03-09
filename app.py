# app.py
from flask import Flask, jsonify
from btc_pro_runner import get_snapshot

app = Flask(__name__)

@app.route("/")
def home():
    return jsonify(get_snapshot())
