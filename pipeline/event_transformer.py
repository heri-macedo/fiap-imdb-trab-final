import re
from typing import Any, Dict


def _extract_numeric_id(value: str) -> str:
    match = re.search(r"(\d+)$", value or "")
    return match.group(1) if match else value


def normalize_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    event_type = str(raw.get("type", "")).strip().lower()
    restaurant_id = str(raw.get("restaurant_id", "")).strip()
    restaurant_num = _extract_numeric_id(restaurant_id)

    ts = int(raw.get("ts") or 0)
    if ts <= 0:
        raise ValueError("Evento sem timestamp válido (ms).")

    event = {
        "type": event_type,
        "ts": ts,
        "user_id": str(raw.get("user_id", "")),
        "restaurant_id": restaurant_id,
        "restaurant_num": restaurant_num,
        "restaurant_name": str(raw.get("restaurant_name", "")),
        "dish_name": str(raw.get("dish_name", "")),
        "dish_id": str(raw.get("dish_id", "")),
        "neighborhood": str(raw.get("neighborhood", "")),
        "lat": float(raw.get("lat", 0.0)),
        "lon": float(raw.get("lon", 0.0)),
        "stars": float(raw.get("stars", 0.0)),
        "cuisine": str(raw.get("cuisine", "")),
    }
    return event


def hash_key(event: Dict[str, Any]) -> str:
    return f"resto:{event['restaurant_num']}"


def ts_key(event: Dict[str, Any], metric: str) -> str:
    return f"ts:resto:{event['restaurant_num']}:{metric}"


def ranking_key(event: Dict[str, Any]) -> str:
    if event["type"] == "view":
        return "ranking:restaurants:views"
    if event["type"] == "order":
        return "ranking:restaurants:orders"
    if event["type"] == "search":
        return "ranking:dishes:searches"
    return ""
