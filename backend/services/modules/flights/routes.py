"""
routes.py — Background flight route resolution cache.
"""

import logging
import time

from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

dynamic_routes_cache: dict = {}
routes_fetch_in_progress: bool = False
ROUTES_CACHE_TTL = 7200
ROUTES_CACHE_MAX = 5000

# Circuit breaker: back off for 10 minutes after a failed batch
_routeset_backoff_until: float = 0
_ROUTESET_BACKOFF_SECONDS = 600


def fetch_routes_background(sampled: list) -> None:
    global dynamic_routes_cache, routes_fetch_in_progress, _routeset_backoff_until
    if routes_fetch_in_progress:
        return
    if time.time() < _routeset_backoff_until:
        return
    routes_fetch_in_progress = True
    try:
        now_ts = time.time()
        stale_keys = [
            k
            for k, v in dynamic_routes_cache.items()
            if now_ts - v.get("_ts", 0) > ROUTES_CACHE_TTL
        ]
        for k in stale_keys:
            del dynamic_routes_cache[k]
        if len(dynamic_routes_cache) > ROUTES_CACHE_MAX:
            sorted_keys = sorted(
                dynamic_routes_cache,
                key=lambda k: dynamic_routes_cache[k].get("_ts", 0),
            )
            for k in sorted_keys[: len(dynamic_routes_cache) - ROUTES_CACHE_MAX]:
                del dynamic_routes_cache[k]

        callsigns_to_query = []
        for f in sampled:
            c_sign = str(f.get("flight", "")).strip()
            if c_sign and c_sign != "UNKNOWN" and c_sign not in dynamic_routes_cache:
                callsigns_to_query.append(
                    {
                        "callsign": c_sign,
                        "lat": f.get("lat", 0),
                        "lng": f.get("lon", 0),
                    }
                )

        batch_size = 100
        batches = [
            callsigns_to_query[i : i + batch_size]
            for i in range(0, len(callsigns_to_query), batch_size)
        ]

        for batch in batches:
            try:
                r = fetch_with_curl(
                    "https://api.adsb.lol/api/0/routeset",
                    method="POST",
                    json_data={"planes": batch},
                    timeout=15,
                )
                if r.status_code in (503, 502, 429):
                    _routeset_backoff_until = time.time() + _ROUTESET_BACKOFF_SECONDS
                    logger.warning(
                        f"routeset {r.status_code} — backing off for {_ROUTESET_BACKOFF_SECONDS}s"
                    )
                    break
                if r.status_code == 200:
                    route_data = r.json()
                    route_list = (
                        route_data.get("value", [])
                        if isinstance(route_data, dict)
                        else route_data
                    )
                    for route in route_list:
                        callsign = route.get("callsign", "")
                        airports = route.get("_airports", [])
                        if airports and len(airports) >= 2:
                            orig_apt = airports[0]
                            dest_apt = airports[-1]
                            dynamic_routes_cache[callsign] = {
                                "orig_name": f"{orig_apt.get('iata', '')}: {orig_apt.get('name', 'Unknown')}",
                                "dest_name": f"{dest_apt.get('iata', '')}: {dest_apt.get('name', 'Unknown')}",
                                "orig_loc": [
                                    orig_apt.get("lon", 0),
                                    orig_apt.get("lat", 0),
                                ],
                                "dest_loc": [
                                    dest_apt.get("lon", 0),
                                    dest_apt.get("lat", 0),
                                ],
                                "_ts": time.time(),
                            }
                time.sleep(0.25)
            except Exception:
                pass
    finally:
        routes_fetch_in_progress = False
