import time

from fastapi import APIRouter
from services.data_fetcher import get_latest_data, source_timestamps

router = APIRouter(prefix="/api")

_start_time = time.time()


@router.get("/health")
async def health_check():
    d = get_latest_data()
    last = d.get("last_updated")
    return {
        "status": "ok",
        "last_updated": last,
        "sources": {
            k: len(v)
            for k, v in d.items()
            if isinstance(v, list) and k != "last_updated"
        },
        "freshness": dict(source_timestamps),
        "uptime_seconds": round(time.time() - _start_time),
    }


@router.get("/health/detail")
async def health_detail():
    d = get_latest_data()
    sources = {}
    for k, v in d.items():
        if k == "last_updated":
            continue
        if isinstance(v, list):
            sources[k] = {
                "type": "list",
                "count": len(v),
                "fresh": source_timestamps.get(k),
            }
        elif isinstance(v, dict):
            sources[k] = {
                "type": "dict",
                "keys": len(v),
                "fresh": source_timestamps.get(k),
            }
        elif v is None:
            sources[k] = {"type": "null", "fresh": source_timestamps.get(k)}
        else:
            sources[k] = {"type": type(v).__name__, "fresh": source_timestamps.get(k)}
    return {
        "status": "ok",
        "uptime_seconds": round(time.time() - _start_time),
        "sources": sources,
    }
