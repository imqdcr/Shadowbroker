"""
fetcher.py — Raw ADS-B data acquisition layer.

Fetches from:
  1. adsb.lol  — primary global coverage (6 regional queries, parallel)
  2. OpenSky   — fallback for Africa / Asia / South America (throttled, OAuth2)
  3. airplanes.live + adsb.fi — blind-spot gap-fill (throttled, 2-min cache)
  4. adsb.lol /v2/mil — military + UAV transponders
"""

import concurrent.futures
import logging
import os
import time

import requests

from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OpenSky OAuth2 client (400 req/day limit → poll every 5 min)
# ---------------------------------------------------------------------------
class OpenSkyClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expires_at = 0.0

    def get_token(self) -> str | None:
        if self.token and time.time() < self.expires_at - 60:
            return self.token
        url = ("https://auth.opensky-network.org/auth/realms/opensky-network"
               "/protocol/openid-connect/token")
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        try:
            r = requests.post(url, data=data, timeout=10)
            if r.status_code == 200:
                res = r.json()
                self.token = res.get("access_token")
                self.expires_at = time.time() + res.get("expires_in", 1800)
                logger.info("OpenSky OAuth2 token refreshed.")
                return self.token
            logger.error(f"OpenSky Auth Failed: {r.status_code} {r.text}")
        except Exception as e:
            logger.error(f"OpenSky Auth Exception: {e}")
        return None


_opensky_client = OpenSkyClient(
    client_id=os.environ.get("OPENSKY_CLIENT_ID", ""),
    client_secret=os.environ.get("OPENSKY_CLIENT_SECRET", ""),
)
_last_opensky_fetch: float = 0.0
_cached_opensky_flights: list = []

# ---------------------------------------------------------------------------
# Supplemental blind-spot regions (Russia / China / Africa)
# ---------------------------------------------------------------------------
_BLIND_SPOT_REGIONS = [
    {"name": "Yekaterinburg", "lat": 56.8, "lon": 60.6,  "radius_nm": 250},
    {"name": "Novosibirsk",   "lat": 55.0, "lon": 82.9,  "radius_nm": 250},
    {"name": "Krasnoyarsk",   "lat": 56.0, "lon": 92.9,  "radius_nm": 250},
    {"name": "Vladivostok",   "lat": 43.1, "lon": 131.9, "radius_nm": 250},
    {"name": "Urumqi",        "lat": 43.8, "lon": 87.6,  "radius_nm": 250},
    {"name": "Chengdu",       "lat": 30.6, "lon": 104.1, "radius_nm": 250},
    {"name": "Lagos-Accra",   "lat": 6.5,  "lon": 3.4,   "radius_nm": 250},
    {"name": "Addis Ababa",   "lat": 9.0,  "lon": 38.7,  "radius_nm": 250},
]
_SUPPLEMENTAL_INTERVAL = 120  # seconds
_last_supplemental_fetch: float = 0.0
_cached_supplemental: list = []

# Primary adsb.lol regions
_PRIMARY_REGIONS = [
    {"lat": 39.8,  "lon": -98.5,  "dist": 2000},  # USA
    {"lat": 50.0,  "lon": 15.0,   "dist": 2000},  # Europe
    {"lat": 35.0,  "lon": 105.0,  "dist": 2000},  # Asia / China
    {"lat": -25.0, "lon": 133.0,  "dist": 2000},  # Australia
    {"lat": 0.0,   "lon": 20.0,   "dist": 2500},  # Africa
    {"lat": -15.0, "lon": -60.0,  "dist": 2000},  # South America
]


def fetch_civilian_raw() -> tuple[list, set]:
    """Return (all_raw_aircraft, seen_hex_set) from adsb.lol + OpenSky + supplemental."""
    global _last_opensky_fetch, _cached_opensky_flights
    global _last_supplemental_fetch, _cached_supplemental

    # --- adsb.lol primary regions (parallel) ---
    def _fetch_region(r):
        url = f"https://api.adsb.lol/v2/lat/{r['lat']}/lon/{r['lon']}/dist/{r['dist']}"
        try:
            res = fetch_with_curl(url, timeout=10)
            if res.status_code == 200:
                return res.json().get("ac", [])
        except Exception as e:
            logger.warning(f"Region fetch failed lat={r['lat']}: {e}")
        return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = pool.map(_fetch_region, _PRIMARY_REGIONS)
    all_flights = []
    for region_flights in results:
        all_flights.extend(region_flights)

    # --- OpenSky fallback (throttled) ---
    now = time.time()
    if now - _last_opensky_fetch > 300:
        token = _opensky_client.get_token()
        if token:
            opensky_regions = [
                {"name": "Africa",       "bbox": {"lamin": -35.0, "lomin": -20.0, "lamax": 38.0,  "lomax": 55.0}},
                {"name": "Asia",         "bbox": {"lamin": 0.0,   "lomin": 30.0,  "lamax": 75.0,  "lomax": 150.0}},
                {"name": "South America","bbox": {"lamin": -60.0, "lomin": -95.0, "lamax": 15.0,  "lomax": -30.0}},
            ]
            new_opensky: list = []
            for os_reg in opensky_regions:
                try:
                    bb = os_reg["bbox"]
                    os_url = (f"https://opensky-network.org/api/states/all"
                              f"?lamin={bb['lamin']}&lomin={bb['lomin']}"
                              f"&lamax={bb['lamax']}&lomax={bb['lomax']}")
                    os_res = requests.get(os_url, headers={"Authorization": f"Bearer {token}"}, timeout=15)
                    if os_res.status_code == 200:
                        states = os_res.json().get("states") or []
                        logger.info(f"OpenSky: {len(states)} states for {os_reg['name']}")
                        for s in states:
                            new_opensky.append({
                                "hex": s[0],
                                "flight": s[1].strip() if s[1] else "UNKNOWN",
                                "r": s[2],
                                "lon": s[5],
                                "lat": s[6],
                                "alt_baro": (s[7] * 3.28084) if s[7] else 0,
                                "track": s[10] or 0,
                                "gs": (s[9] * 1.94384) if s[9] else 0,
                                "t": "Unknown",
                                "is_opensky": True,
                            })
                    else:
                        logger.warning(f"OpenSky {os_reg['name']} failed: {os_res.status_code}")
                except Exception as ex:
                    logger.error(f"OpenSky error for {os_reg['name']}: {ex}")
            _cached_opensky_flights = new_opensky
            _last_opensky_fetch = now

    # Deduplicate: adsb.lol is primary; OpenSky only fills gaps
    seen_hex: set = set()
    for f in all_flights:
        h = f.get("hex")
        if h:
            seen_hex.add(h.lower().strip())
    for osf in _cached_opensky_flights:
        h = osf.get("hex")
        if h and h.lower().strip() not in seen_hex:
            all_flights.append(osf)
            seen_hex.add(h.lower().strip())

    # --- Supplemental gap-fill ---
    try:
        gap = _fetch_supplemental(seen_hex)
        for f in gap:
            all_flights.append(f)
            h = f.get("hex", "").lower().strip()
            if h:
                seen_hex.add(h)
        if gap:
            logger.info(f"Gap-fill: +{len(gap)} aircraft")
    except Exception as e:
        logger.warning(f"Supplemental fetch failed (non-fatal): {e}")

    return all_flights, seen_hex


def _fetch_supplemental(seen_hex: set) -> list:
    global _last_supplemental_fetch, _cached_supplemental

    now = time.time()
    if now - _last_supplemental_fetch < _SUPPLEMENTAL_INTERVAL:
        return [f for f in _cached_supplemental
                if f.get("hex", "").lower().strip() not in seen_hex]

    new_supplemental: list = []
    supplemental_hex: set = set()

    def _fetch_airplaneslive(region):
        try:
            url = (f"https://api.airplanes.live/v2/point/"
                   f"{region['lat']}/{region['lon']}/{region['radius_nm']}")
            res = fetch_with_curl(url, timeout=10)
            if res.status_code == 200:
                return res.json().get("ac", [])
        except Exception as e:
            logger.debug(f"airplanes.live {region['name']} failed: {e}")
        return []

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(_fetch_airplaneslive, _BLIND_SPOT_REGIONS))
        for region_flights in results:
            for f in region_flights:
                h = f.get("hex", "").lower().strip()
                if h and h not in seen_hex and h not in supplemental_hex:
                    f["supplemental_source"] = "airplanes.live"
                    new_supplemental.append(f)
                    supplemental_hex.add(h)
    except Exception as e:
        logger.warning(f"airplanes.live supplemental fetch failed: {e}")

    ap_count = len(new_supplemental)

    try:
        for region in _BLIND_SPOT_REGIONS:
            try:
                url = (f"https://opendata.adsb.fi/api/v3/lat/"
                       f"{region['lat']}/lon/{region['lon']}/dist/{region['radius_nm']}")
                res = fetch_with_curl(url, timeout=10)
                if res.status_code == 200:
                    for f in res.json().get("ac", []):
                        h = f.get("hex", "").lower().strip()
                        if h and h not in seen_hex and h not in supplemental_hex:
                            f["supplemental_source"] = "adsb.fi"
                            new_supplemental.append(f)
                            supplemental_hex.add(h)
            except Exception as e:
                logger.debug(f"adsb.fi {region['name']} failed: {e}")
            time.sleep(1.1)
    except Exception as e:
        logger.warning(f"adsb.fi supplemental fetch failed: {e}")

    fi_count = len(new_supplemental) - ap_count
    _cached_supplemental = new_supplemental
    _last_supplemental_fetch = now
    logger.info(f"Supplemental: +{len(new_supplemental)} aircraft "
                f"(airplanes.live: {ap_count}, adsb.fi: {fi_count})")
    return new_supplemental


def fetch_military_raw() -> list:
    """Fetch military transponders from adsb.lol /v2/mil."""
    try:
        response = fetch_with_curl("https://api.adsb.lol/v2/mil", timeout=10)
        if response.status_code == 200:
            return response.json().get("ac", [])
    except Exception as e:
        logger.error(f"Military fetch error: {e}")
    return []
