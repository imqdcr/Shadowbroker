import logging
import os

logging.basicConfig(level=logging.INFO)
logging.getLogger("apscheduler.executors.default").setLevel(logging.DEBUG)
logging.getLogger("apscheduler.scheduler").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Docker Swarm Secrets support
# For each VAR below, if VAR_FILE is set (e.g. AIS_API_KEY_FILE=/run/secrets/AIS_API_KEY),
# the file is read and its trimmed content is placed into VAR.
# This MUST run before service imports — modules read os.environ at import time.
# ---------------------------------------------------------------------------
_SECRET_VARS = [
    "AIS_API_KEY",
    "OPENSKY_CLIENT_ID",
    "OPENSKY_CLIENT_SECRET",
    "LTA_ACCOUNT_KEY",
    "CORS_ORIGINS",
]

for _var in _SECRET_VARS:
    _file_var = f"{_var}_FILE"
    _file_path = os.environ.get(_file_var)
    if _file_path:
        try:
            with open(_file_path, "r") as _f:
                _value = _f.read().strip()
            if _value:
                os.environ[_var] = _value
                logger.info(f"Loaded secret {_var} from {_file_path}")
            else:
                logger.warning(f"Secret file {_file_path} for {_var} is empty")
        except FileNotFoundError:
            logger.error(f"Secret file {_file_path} for {_var} not found")
        except Exception as _e:
            logger.error(f"Failed to read secret file {_file_path} for {_var}: {_e}")

import hashlib
import json as json_mod
import socket
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from services.data_fetcher import (get_latest_data, source_timestamps,
                                   start_scheduler, stop_scheduler)
from services.module_loader import loader
from services.modules.ships.ais_stream import start_ais_stream, stop_ais_stream
from services.modules.ships.carrier_tracker import (start_carrier_tracker,
                                                    stop_carrier_tracker)


def _build_cors_origins():
    """Build a CORS origins whitelist: localhost + LAN IPs + env overrides.
    Falls back to wildcard only if auto-detection fails entirely."""
    origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]
    # Add this machine's LAN IPs (covers common home/office setups)
    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ip = info[4][0]
            if ip not in ("127.0.0.1", "0.0.0.0"):
                origins.append(f"http://{ip}:3000")
                origins.append(f"http://{ip}:8000")
    except Exception:
        pass
    # Allow user override via CORS_ORIGINS env var (comma-separated)
    extra = os.environ.get("CORS_ORIGINS", "")
    if extra:
        origins.extend([o.strip() for o in extra.split(",") if o.strip()])
    return list(set(origins))  # deduplicate


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start background data fetching, AIS stream, and carrier tracker
    start_carrier_tracker()
    start_ais_stream()
    start_scheduler()
    yield
    # Shutdown: Stop all background services
    stop_ais_stream()
    stop_scheduler()
    stop_carrier_tracker()


app = FastAPI(title="Live Risk Dashboard API", lifespan=lifespan)

from fastapi.middleware.gzip import GZipMiddleware

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_build_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from services.data_fetcher import update_all_data


@app.get("/api/refresh")
async def force_refresh():
    # Force an immediate synchronous update of the data payload
    import threading

    t = threading.Thread(target=update_all_data)
    t.start()
    return {"status": "refreshing in background"}


@app.get("/api/live-data")
async def live_data():
    return get_latest_data()


def _etag_response(request: Request, payload: dict, prefix: str = "", default=None):
    """Serialize once, hash the bytes for ETag, return 304 or full response."""
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


@app.get("/api/live-data/fast")
async def live_data_fast(request: Request):
    d = get_latest_data()
    keys = loader.build_fast_keys()
    payload = {k: d.get(k) for k in keys}
    payload["freshness"] = dict(source_timestamps)
    return _etag_response(request, payload, prefix="fast|")


@app.get("/api/live-data/slow")
async def live_data_slow(request: Request):
    d = get_latest_data()
    keys = loader.build_slow_keys()
    payload = {k: d.get(k) for k in keys}
    payload["last_updated"] = d.get("last_updated")
    payload["freshness"] = dict(source_timestamps)
    return _etag_response(request, payload, prefix="slow|", default=str)


@app.get("/api/debug-latest")
async def debug_latest_data():
    return list(get_latest_data().keys())


@app.get("/api/health")
async def health_check():
    import time

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


_start_time = __import__("time").time()


@app.get("/api/health/detail")
async def health_detail():
    import time

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


from services.radio_intercept import (find_nearest_openmhz_system,
                                      get_openmhz_systems,
                                      get_recent_openmhz_calls,
                                      get_top_broadcastify_feeds)


@app.get("/api/radio/top")
async def get_top_radios():
    return get_top_broadcastify_feeds()


@app.get("/api/radio/openmhz/systems")
async def api_get_openmhz_systems():
    return get_openmhz_systems()


@app.get("/api/radio/openmhz/calls/{sys_name}")
async def api_get_openmhz_calls(sys_name: str):
    return get_recent_openmhz_calls(sys_name)


@app.get("/api/radio/nearest")
async def api_get_nearest_radio(lat: float, lng: float):
    return find_nearest_openmhz_system(lat, lng)


from services.radio_intercept import find_nearest_openmhz_systems_list


@app.get("/api/radio/nearest-list")
async def api_get_nearest_radios_list(lat: float, lng: float, limit: int = 5):
    return find_nearest_openmhz_systems_list(lat, lng, limit=limit)


from services.network_utils import fetch_with_curl


@app.get("/api/route/{callsign}")
async def get_flight_route(callsign: str, lat: float = 0.0, lng: float = 0.0):
    r = fetch_with_curl(
        "https://api.adsb.lol/api/0/routeset",
        method="POST",
        json_data={"planes": [{"callsign": callsign, "lat": lat, "lng": lng}]},
        timeout=10,
    )
    if r and r.status_code == 200:
        data = r.json()
        route_list = []
        if isinstance(data, dict):
            route_list = data.get("value", [])
        elif isinstance(data, list):
            route_list = data

        if route_list and len(route_list) > 0:
            route = route_list[0]
            airports = route.get("_airports", [])
            if len(airports) >= 2:
                orig = airports[0]
                dest = airports[-1]
                return {
                    "orig_loc": [orig.get("lon", 0), orig.get("lat", 0)],
                    "dest_loc": [dest.get("lon", 0), dest.get("lat", 0)],
                    "origin_name": f"{orig.get('iata', '') or orig.get('icao', '')}: {orig.get('name', 'Unknown')}",
                    "dest_name": f"{dest.get('iata', '') or dest.get('icao', '')}: {dest.get('name', 'Unknown')}",
                }
    return {}


from services.region_dossier import get_region_dossier


@app.get("/api/region-dossier")
def api_region_dossier(lat: float, lng: float):
    """Sync def so FastAPI runs it in a threadpool — prevents blocking the event loop."""
    return get_region_dossier(lat, lng)


from services.sentinel_search import search_sentinel2_scene


@app.get("/api/sentinel2/search")
def api_sentinel2_search(lat: float, lng: float):
    """Search for latest Sentinel-2 imagery at a point. Sync for threadpool execution."""
    return search_sentinel2_scene(lat, lng)


from pydantic import BaseModel
# ---------------------------------------------------------------------------
# API Settings — key registry & management
# ---------------------------------------------------------------------------
from services.api_settings import get_api_keys, update_api_key


class ApiKeyUpdate(BaseModel):
    env_key: str
    value: str


@app.get("/api/settings/api-keys")
async def api_get_keys():
    return get_api_keys()


@app.put("/api/settings/api-keys")
async def api_update_key(body: ApiKeyUpdate):
    ok = update_api_key(body.env_key, body.value)
    if ok:
        return {"status": "updated", "env_key": body.env_key}
    return {"status": "error", "message": "Failed to update .env file"}


# ---------------------------------------------------------------------------
# News Feed Configuration
# ---------------------------------------------------------------------------
from services.modules.news.feed_config import (get_feeds, reset_feeds,
                                               save_feeds)


@app.get("/api/settings/news-feeds")
async def api_get_news_feeds():
    return get_feeds()


@app.put("/api/settings/news-feeds")
async def api_save_news_feeds(request: Request):
    body = await request.json()
    ok = save_feeds(body)
    if ok:
        return {"status": "updated", "count": len(body)}
    return Response(
        content=json_mod.dumps(
            {
                "status": "error",
                "message": "Validation failed (max 20 feeds, each needs name/url/weight 1-5)",
            }
        ),
        status_code=400,
        media_type="application/json",
    )


@app.post("/api/settings/news-feeds/reset")
async def api_reset_news_feeds():
    ok = reset_feeds()
    if ok:
        return {"status": "reset", "feeds": get_feeds()}
    return {"status": "error", "message": "Failed to reset feeds"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

# Application successfully initialized with background scraping tasks
