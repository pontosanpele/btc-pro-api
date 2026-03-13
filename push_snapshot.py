import json
from datetime import datetime
from pathlib import Path

import requests

UPLOAD_URL = "https://btc-pro-api.onrender.com/upload"


def _now():
    return datetime.now().isoformat(timespec="seconds")


def _ensure_clean_python_sources() -> None:
    bad_prefixes = ("@@", "<<<<<<<", "=======", ">>>>>>>")
    for py_file in Path(".").glob("*.py"):
        with py_file.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f, start=1):
                if line.startswith(bad_prefixes):
                    raise RuntimeError(
                        f"Invalid patch marker found in {py_file} line {i}: {line.strip()}. "
                        "Please restore the file from git before running push_snapshot.py."
                    )


def main():
    try:
        _ensure_clean_python_sources()
        from btc_pro_strategy import build_snapshot

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
