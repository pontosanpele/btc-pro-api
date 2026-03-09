import json
from btc_pro_strategy import build_snapshot
if __name__ == '__main__':
    snapshot=build_snapshot()
    print('===API_SNAPSHOT_START===')
    print(json.dumps(snapshot,ensure_ascii=False))
    print('===API_SNAPSHOT_END===')
