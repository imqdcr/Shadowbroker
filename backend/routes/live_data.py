import hashlib
import json as json_mod
import threading

from fastapi import APIRouter, Request, Response
from services.data_fetcher import (get_latest_data, source_timestamps,
                                   update_all_data)
from services.module_loader import loader

router = APIRouter(prefix="/api")


def _etag_response(request: Request, payload: dict, prefix: str = "", default=None):
    content = json_mod.dumps(payload, default=default)
    etag = hashlib.md5(f"{prefix}{content[:256]}".encode()).hexdigest()[:16]
    if request.headers.get("if-none-match") == etag:
        return Response(
            status_code=304, headers={"ETag": etag, "Cache-Control": "no-cache"}
        )
    return Response(
        content=content,
        media_type="application/json",
        headers={"ETag": etag, "Cache-Control": "no-cache"},
    )


@router.get("/refresh")
async def force_refresh():
    t = threading.Thread(target=update_all_data)
    t.start()
    return {"status": "refreshing in background"}


@router.get("/live-data")
async def live_data():
    return get_latest_data()


@router.get("/live-data/fast")
async def live_data_fast(request: Request):
    d = get_latest_data()
    keys = loader.build_fast_keys()
    payload = {k: d.get(k) for k in keys}
    payload["freshness"] = dict(source_timestamps)
    return _etag_response(request, payload, prefix="fast|")


@router.get("/live-data/slow")
async def live_data_slow(request: Request):
    d = get_latest_data()
    keys = loader.build_slow_keys()
    payload = {k: d.get(k) for k in keys}
    payload["last_updated"] = d.get("last_updated")
    payload["freshness"] = dict(source_timestamps)
    return _etag_response(request, payload, prefix="slow|", default=str)


@router.get("/debug-latest")
async def debug_latest_data():
    return list(get_latest_data().keys())
