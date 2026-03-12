import json
from datetime import datetime
import requests
from btc_pro_strategy import build_snapshot

UPLOAD_URL = "https://btc-pro-api.onrender.com/upload"


def _now():
    return datetime.now().isoformat(timespec="seconds")


def main():
    try:
        snapshot = build_snapshot()
        r = requests.post(UPLOAD_URL, json=snapshot, timeout=30)
        print(f"{_now()} STATUS {r.status_code}")
        try:
            print(json.dumps(r.json(), ensure_ascii=False))
        except Exception:
            print(r.text)
    except Exception as e:
        print(f"{_now()} ERROR {type(e).__name__}: {e}")
        raise


if __name__ == "__main__":
    main()
