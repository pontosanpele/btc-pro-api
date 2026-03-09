from typing import Callable, Dict, Optional, Tuple

from btc_pro_config import FIELD_SOURCE_PRIORITY


def resolve_route(field_name: str, providers: Dict[str, Callable[[], dict]]) -> Tuple[dict, Optional[str]]:
    """Return first successful provider payload based on configured priority."""
    for source in FIELD_SOURCE_PRIORITY.get(field_name, []):
        fn = providers.get(source)
        if fn is None:
            continue
        try:
            payload = fn() or {}
        except Exception:
            continue
        if payload:
            return payload, source
    return {}, None


def attach_source(payload: dict, source_name: Optional[str], field_prefix: str) -> dict:
    out = dict(payload or {})
    out[f"source_{field_prefix}"] = source_name
    return out
