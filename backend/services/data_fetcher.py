import requests
import logging
from services.network_utils import fetch_with_curl
import os
import re
import math
import json
import time
from pathlib import Path
import threading
from apscheduler.schedulers.background import BackgroundScheduler
import concurrent.futures
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
from services.cctv_pipeline import init_db
from services.module_loader import loader

logger = logging.getLogger(__name__)


# Pre-compiled regex patterns for airline code extraction (used in hot loop)
_RE_AIRLINE_CODE_1 = re.compile(r'^([A-Z]{3})\d')
_RE_AIRLINE_CODE_2 = re.compile(r'^([A-Z]{3})[A-Z\d]')


# ---------------------------------------------------------------------------
# OpenSky Network API Client (OAuth2)
# ---------------------------------------------------------------------------
class OpenSkyClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expires_at = 0

    def get_token(self):
        import time
        if self.token and time.time() < self.expires_at - 60:
            return self.token
        
        url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        try:
            r = requests.post(url, data=data, timeout=10)
            if r.status_code == 200:
                res = r.json()
                self.token = res.get("access_token")
                self.expires_at = time.time() + res.get("expires_in", 1800)
                logger.info("OpenSky OAuth2 token refreshed.")
                return self.token
            else:
                logger.error(f"OpenSky Auth Failed: {r.status_code} {r.text}")
        except Exception as e:
            logger.error(f"OpenSky Auth Exception: {e}")
        return None

# User provided credentials
opensky_client = OpenSkyClient(
    client_id=os.environ.get("OPENSKY_CLIENT_ID", ""),
    client_secret=os.environ.get("OPENSKY_CLIENT_SECRET", "")
)

# Throttling and caching for OpenSky to observe the 400 req/day limit
last_opensky_fetch = 0
cached_opensky_flights = []

# ---------------------------------------------------------------------------
# Supplemental ADS-B sources for blind-spot gap-filling (Russia/China/Africa)
# These aggregators have different feeder pools than adsb.lol and can surface
# aircraft invisible to our primary source.  Only gap-fill planes are kept.
# ---------------------------------------------------------------------------
_BLIND_SPOT_REGIONS = [
    {"name": "Yekaterinburg",  "lat": 56.8, "lon": 60.6,  "radius_nm": 250},
    {"name": "Novosibirsk",   "lat": 55.0, "lon": 82.9,  "radius_nm": 250},
    {"name": "Krasnoyarsk",   "lat": 56.0, "lon": 92.9,  "radius_nm": 250},
    {"name": "Vladivostok",   "lat": 43.1, "lon": 131.9, "radius_nm": 250},
    {"name": "Urumqi",        "lat": 43.8, "lon": 87.6,  "radius_nm": 250},
    {"name": "Chengdu",       "lat": 30.6, "lon": 104.1, "radius_nm": 250},
    {"name": "Lagos-Accra",   "lat": 6.5,  "lon": 3.4,   "radius_nm": 250},
    {"name": "Addis Ababa",   "lat": 9.0,  "lon": 38.7,  "radius_nm": 250},
]
_SUPPLEMENTAL_FETCH_INTERVAL = 120  # seconds — only query every 2 min
last_supplemental_fetch = 0
cached_supplemental_flights = []



# In-memory store
latest_data = {
    "last_updated": None,
    "news": [],
    "stocks": {},
    "oil": {},
    "flights": [],
    "ships": [],
    "military_flights": [],
    "tracked_flights": [],
    "cctv": [],
    "weather": None,
    # bikeshare removed per user request
    "traffic": [],
    "earthquakes": [],
    "uavs": [],
    "frontlines": None,
    "gdelt": [],
    "liveuamap": [],
    "kiwisdr": [],
    "space_weather": None,
    "internet_outages": [],
    "firms_fires": [],
    "datacenters": []
}

# Per-source freshness timestamps — updated each time a fetch function completes successfully
source_timestamps = {}

def _mark_fresh(*keys):
    """Record the current UTC time for one or more data source keys."""
    now = datetime.utcnow().isoformat()
    for k in keys:
        source_timestamps[k] = now

# Thread lock for safe reads/writes to latest_data
_data_lock = threading.Lock()

# Load modules from modules.yaml (enabled=true entries only).
# Must happen after latest_data is defined so init_latest_data can write defaults.
loader.load()
loader.init_latest_data(latest_data)

# ---------------------------------------------------------------------------
# Plane-Alert DB — load tracked aircraft from JSON on startup
# ---------------------------------------------------------------------------

# Exact category → color mapping for all 53 known categories.
# O(1) dict lookup — no keyword scanning, no false positives.
_CATEGORY_COLOR: dict[str, str] = {
    # YELLOW — Military / Intelligence / Defense
    "USAF": "yellow",
    "Other Air Forces": "yellow",
    "Toy Soldiers": "yellow",
    "Oxcart": "yellow",
    "United States Navy": "yellow",
    "GAF": "yellow",
    "Hired Gun": "yellow",
    "United States Marine Corps": "yellow",
    "Gunship": "yellow",
    "RAF": "yellow",
    "Other Navies": "yellow",
    "Special Forces": "yellow",
    "Zoomies": "yellow",
    "Royal Navy Fleet Air Arm": "yellow",
    "Army Air Corps": "yellow",
    "Aerobatic Teams": "yellow",
    "UAV": "yellow",
    "Ukraine": "yellow",
    "Nuclear": "yellow",
    # LIME — Emergency / Medical / Rescue / Fire
    "Flying Doctors": "#32cd32",
    "Aerial Firefighter": "#32cd32",
    "Coastguard": "#32cd32",
    # BLUE — Government / Law Enforcement / Civil
    "Police Forces": "blue",
    "Governments": "blue",
    "Quango": "blue",
    "UK National Police Air Service": "blue",
    "CAP": "blue",
    # BLACK — Privacy / PIA
    "PIA": "black",
    # RED — Dictator / Oligarch
    "Dictator Alert": "red",
    "Da Comrade": "red",
    "Oligarch": "red",
    # HOT PINK — High Value Assets / VIP / Celebrity
    "Head of State": "#ff1493",
    "Royal Aircraft": "#ff1493",
    "Don't you know who I am?": "#ff1493",
    "As Seen on TV": "#ff1493",
    "Bizjets": "#ff1493",
    "Vanity Plate": "#ff1493",
    "Football": "#ff1493",
    # ORANGE — Joe Cool
    "Joe Cool": "orange",
    # WHITE — Climate Crisis
    "Climate Crisis": "white",
    # PURPLE — General Tracked / Other Notable
    "Historic": "purple",
    "Jump Johnny Jump": "purple",
    "Ptolemy would be proud": "purple",
    "Distinctive": "purple",
    "Dogs with Jobs": "purple",
    "You came here in that thing?": "purple",
    "Big Hello": "purple",
    "Watch Me Fly": "purple",
    "Perfectly Serviceable Aircraft": "purple",
    "Jesus he Knows me": "purple",
    "Gas Bags": "purple",
    "Radiohead": "purple",
}

def _category_to_color(cat: str) -> str:
    """O(1) exact lookup. Unknown categories default to purple."""
    return _CATEGORY_COLOR.get(cat, "purple")

_PLANE_ALERT_DB: dict = {}

# ---------------------------------------------------------------------------
# POTUS Fleet — override colors and operator names for presidential aircraft.
# These are hardcoded ICAO hexes verified against FAA registry + plane-alert.
# ---------------------------------------------------------------------------
_POTUS_FLEET: dict[str, dict] = {
    # Air Force One — Boeing VC-25A (747-200B)
    "ADFDF8": {"color": "#ff1493", "operator": "Air Force One (82-8000)", "category": "Head of State", "wiki": "Air_Force_One", "fleet": "AF1"},
    "ADFDF9": {"color": "#ff1493", "operator": "Air Force One (92-9000)", "category": "Head of State", "wiki": "Air_Force_One", "fleet": "AF1"},
    # Air Force Two — Boeing C-32A (757-200)
    "ADFEB7": {"color": "blue", "operator": "Air Force Two (98-0001)", "category": "Governments", "wiki": "Air_Force_Two", "fleet": "AF2"},
    "ADFEB8": {"color": "blue", "operator": "Air Force Two (98-0002)", "category": "Governments", "wiki": "Air_Force_Two", "fleet": "AF2"},
    "ADFEB9": {"color": "blue", "operator": "Air Force Two (99-0003)", "category": "Governments", "wiki": "Air_Force_Two", "fleet": "AF2"},
    "ADFEBA": {"color": "blue", "operator": "Air Force Two (99-0004)", "category": "Governments", "wiki": "Air_Force_Two", "fleet": "AF2"},
    "AE4AE6": {"color": "blue", "operator": "Air Force Two (09-0015)", "category": "Governments", "wiki": "Air_Force_Two", "fleet": "AF2"},
    "AE4AE8": {"color": "blue", "operator": "Air Force Two (09-0016)", "category": "Governments", "wiki": "Air_Force_Two", "fleet": "AF2"},
    "AE4AEA": {"color": "blue", "operator": "Air Force Two (09-0017)", "category": "Governments", "wiki": "Air_Force_Two", "fleet": "AF2"},
    "AE4AEC": {"color": "blue", "operator": "Air Force Two (19-0018)", "category": "Governments", "wiki": "Air_Force_Two", "fleet": "AF2"},
    # Marine One — VH-3D Sea King / VH-92A Patriot
    "AE0865": {"color": "#ff1493", "operator": "Marine One (VH-3D)", "category": "Head of State", "wiki": "Marine_One", "fleet": "M1"},
    "AE5E76": {"color": "#ff1493", "operator": "Marine One (VH-92A)", "category": "Head of State", "wiki": "Marine_One", "fleet": "M1"},
    "AE5E77": {"color": "#ff1493", "operator": "Marine One (VH-92A)", "category": "Head of State", "wiki": "Marine_One", "fleet": "M1"},
    "AE5E79": {"color": "#ff1493", "operator": "Marine One (VH-92A)", "category": "Head of State", "wiki": "Marine_One", "fleet": "M1"},
}

def _load_plane_alert_db():
    """Load plane_alert_db.json (exported from SQLite) into memory."""
    global _PLANE_ALERT_DB
    json_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "plane_alert_db.json"
    )
    if not os.path.exists(json_path):
        logger.warning(f"Plane-Alert DB not found at {json_path}")
        return
    try:
        with open(json_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        for icao_hex, info in raw.items():
            info["color"] = _category_to_color(info.get("category", ""))
            # Apply POTUS fleet overrides (correct colors + clean operator names)
            override = _POTUS_FLEET.get(icao_hex)
            if override:
                info["color"] = override["color"]
                info["operator"] = override["operator"]
                info["category"] = override["category"]
                info["wiki"] = override.get("wiki", "")
                info["potus_fleet"] = override.get("fleet", "")
            _PLANE_ALERT_DB[icao_hex] = info
        logger.info(f"Plane-Alert DB loaded: {len(_PLANE_ALERT_DB)} aircraft")
    except Exception as e:
        logger.error(f"Failed to load Plane-Alert DB: {e}")

_load_plane_alert_db()

def enrich_with_plane_alert(flight: dict) -> dict:
    """If flight's icao24 is in the Plane-Alert DB, add alert metadata."""
    icao = flight.get("icao24", "").strip().upper()
    if icao and icao in _PLANE_ALERT_DB:
        info = _PLANE_ALERT_DB[icao]
        flight["alert_category"] = info["category"]
        flight["alert_color"] = info["color"]
        flight["alert_operator"] = info["operator"]
        flight["alert_type"] = info["ac_type"]
        flight["alert_tags"] = info["tags"]
        flight["alert_link"] = info["link"]
        if info.get("wiki"):
            flight["alert_wiki"] = info["wiki"]
        if info.get("potus_fleet"):
            flight["potus_fleet"] = info["potus_fleet"]
        if info["registration"]:
            flight["registration"] = info["registration"]

    return flight

# (json imported at module top)
_TRACKED_NAMES_DB: dict = {} # Map from uppercase registration to {name, category}

def _load_tracked_names():
    global _TRACKED_NAMES_DB
    json_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "tracked_names.json"
    )
    if not os.path.exists(json_path):
        return
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # data has:
            # "names": [ {"name": "...", "category": "..."} ]
            # "details": { "Name": { "category": "...", "registrations": ["..."] } }
            for name, info in data.get("details", {}).items():
                cat = info.get("category", "Other")
                for reg in info.get("registrations", []):
                    reg_clean = reg.strip().upper()
                    if reg_clean:
                        _TRACKED_NAMES_DB[reg_clean] = {"name": name, "category": cat}
        logger.info(f"Tracked Names DB loaded: {len(_TRACKED_NAMES_DB)} registrations")
    except Exception as e:
        logger.error(f"Failed to load Tracked Names DB: {e}")

_load_tracked_names()

def enrich_with_tracked_names(flight: dict) -> dict:
    """If flight's registration matches our Excel extraction, tag it as tracked."""
    # POTUS fleet overrides are authoritative — never let Excel overwrite them
    icao = flight.get("icao24", "").strip().upper()
    if icao in _POTUS_FLEET:
        return flight

    reg = flight.get("registration", "").strip().upper()
    callsign = flight.get("callsign", "").strip().upper()

    match = None
    if reg and reg in _TRACKED_NAMES_DB:
        match = _TRACKED_NAMES_DB[reg]
    elif callsign and callsign in _TRACKED_NAMES_DB:
        match = _TRACKED_NAMES_DB[callsign]

    if match:
        name = match["name"]
        # Let Excel take precedence as it has cleaner individual names (e.g. Elon Musk instead of FALCON LANDING LLC).
        flight["alert_operator"] = name
        flight["alert_category"] = match["category"]
        
        # Override pink default if the name implies a specific function
        name_lower = name.lower()
        is_gov = any(w in name_lower for w in ['state of ', 'government', 'republic', 'ministry', 'department', 'federal', 'cia'])
        is_law = any(w in name_lower for w in ['police', 'marshal', 'sheriff', 'douane', 'customs', 'patrol', 'gendarmerie', 'guardia', 'law enforcement'])
        is_med = any(w in name_lower for w in ['fire', 'bomberos', 'ambulance', 'paramedic', 'medevac', 'rescue', 'hospital', 'medical', 'lifeflight'])
        
        if is_gov or is_law:
            flight["alert_color"] = "blue"
        elif is_med:
            flight["alert_color"] = "#32cd32"  # lime
        elif "alert_color" not in flight:
            flight["alert_color"] = "pink"

    return flight




dynamic_routes_cache = {}  # callsign -> {data..., _ts: timestamp}
routes_fetch_in_progress = False
ROUTES_CACHE_TTL = 7200  # 2 hours
ROUTES_CACHE_MAX = 5000

def fetch_routes_background(sampled):
    global dynamic_routes_cache, routes_fetch_in_progress
    if routes_fetch_in_progress:
        return
    routes_fetch_in_progress = True
    
    try:
        # Prune stale entries (older than 2 hours) and cap at max size
        now_ts = time.time()
        stale_keys = [k for k, v in dynamic_routes_cache.items() if now_ts - v.get('_ts', 0) > ROUTES_CACHE_TTL]
        for k in stale_keys:
            del dynamic_routes_cache[k]
        if len(dynamic_routes_cache) > ROUTES_CACHE_MAX:
            # Remove oldest entries
            sorted_keys = sorted(dynamic_routes_cache, key=lambda k: dynamic_routes_cache[k].get('_ts', 0))
            for k in sorted_keys[:len(dynamic_routes_cache) - ROUTES_CACHE_MAX]:
                del dynamic_routes_cache[k]

        callsigns_to_query = []
        for f in sampled:
            c_sign = str(f.get("flight", "")).strip()
            if c_sign and c_sign != "UNKNOWN":
                callsigns_to_query.append({
                    "callsign": c_sign,
                    "lat": f.get("lat", 0),
                    "lng": f.get("lon", 0)
                })
        
        batch_size = 100
        batches = [callsigns_to_query[i:i+batch_size] for i in range(0, len(callsigns_to_query), batch_size)]
        
        for batch in batches:
            try:
                r = fetch_with_curl("https://api.adsb.lol/api/0/routeset", method="POST", json_data={"planes": batch}, timeout=15)
                if r.status_code == 200:
                    route_data = r.json()
                    route_list = []
                    if isinstance(route_data, dict):
                        route_list = route_data.get("value", [])
                    elif isinstance(route_data, list):
                        route_list = route_data
                        
                    for route in route_list:
                        callsign = route.get("callsign", "")
                        airports = route.get("_airports", [])
                        if airports and len(airports) >= 2:
                            orig_apt = airports[0]
                            dest_apt = airports[-1]
                            dynamic_routes_cache[callsign] = {
                                "orig_name": f"{orig_apt.get('iata', '')}: {orig_apt.get('name', 'Unknown')}",
                                "dest_name": f"{dest_apt.get('iata', '')}: {dest_apt.get('name', 'Unknown')}",
                                "orig_loc": [orig_apt.get("lon", 0), orig_apt.get("lat", 0)],
                                "dest_loc": [dest_apt.get("lon", 0), dest_apt.get("lat", 0)],
                                "_ts": time.time(),
                            }
                time.sleep(0.25) # Throttle strictly beneath 10 requests / second limit
            except Exception:
                pass
    finally:
        routes_fetch_in_progress = False

# Helicopter type codes (backend classification)
_HELI_TYPES_BACKEND = {
    "R22", "R44", "R66", "B06", "B06T", "B204", "B205", "B206", "B212", "B222", "B230",
    "B407", "B412", "B427", "B429", "B430", "B505", "B525",
    "AS32", "AS35", "AS50", "AS55", "AS65",
    "EC20", "EC25", "EC30", "EC35", "EC45", "EC55", "EC75",
    "H125", "H130", "H135", "H145", "H155", "H160", "H175", "H215", "H225",
    "S55", "S58", "S61", "S64", "S70", "S76", "S92",
    "A109", "A119", "A139", "A169", "A189", "AW09",
    "MD52", "MD60", "MDHI", "MD90", "NOTR",
    "B47G", "HUEY", "GAMA", "CABR", "EXE",
}


def _fetch_supplemental_sources(seen_hex: set) -> list:
    """Fetch from airplanes.live and adsb.fi to fill blind-spot gaps.

    Only returns aircraft whose ICAO hex is NOT already in seen_hex.
    Throttled to run every _SUPPLEMENTAL_FETCH_INTERVAL seconds.
    Fully wrapped in try/except — returns [] on any failure.
    """
    global last_supplemental_fetch, cached_supplemental_flights

    now = time.time()
    if now - last_supplemental_fetch < _SUPPLEMENTAL_FETCH_INTERVAL:
        # Return cached results, but still filter against current seen_hex
        return [f for f in cached_supplemental_flights
                if f.get("hex", "").lower().strip() not in seen_hex]

    new_supplemental = []
    supplemental_hex = set()  # track hex within supplemental to avoid internal dupes

    # --- airplanes.live (parallel, all hotspots) ---
    def _fetch_airplaneslive(region):
        try:
            url = (f"https://api.airplanes.live/v2/point/"
                   f"{region['lat']}/{region['lon']}/{region['radius_nm']}")
            res = fetch_with_curl(url, timeout=10)
            if res.status_code == 200:
                data = res.json()
                return data.get("ac", [])
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

    # --- adsb.fi (sequential, 1.1s between requests to respect 1 req/sec limit) ---
    try:
        for region in _BLIND_SPOT_REGIONS:
            try:
                url = (f"https://opendata.adsb.fi/api/v3/lat/"
                       f"{region['lat']}/lon/{region['lon']}/dist/{region['radius_nm']}")
                res = fetch_with_curl(url, timeout=10)
                if res.status_code == 200:
                    data = res.json()
                    for f in data.get("ac", []):
                        h = f.get("hex", "").lower().strip()
                        if h and h not in seen_hex and h not in supplemental_hex:
                            f["supplemental_source"] = "adsb.fi"
                            new_supplemental.append(f)
                            supplemental_hex.add(h)
            except Exception as e:
                logger.debug(f"adsb.fi {region['name']} failed: {e}")
            time.sleep(1.1)  # Rate limit: 1 req/sec
    except Exception as e:
        logger.warning(f"adsb.fi supplemental fetch failed: {e}")

    fi_count = len(new_supplemental) - ap_count

    cached_supplemental_flights = new_supplemental
    last_supplemental_fetch = now
    if new_supplemental:
        _mark_fresh("supplemental_flights")

    logger.info(f"Supplemental: +{len(new_supplemental)} new aircraft from blind-spot "
                f"hotspots (airplanes.live: {ap_count}, adsb.fi: {fi_count})")

    return new_supplemental


def fetch_flights():
    # OpenSky Network public API for flights. We want to demonstrate global coverage.
    flights = []
    try:
        # Sample flights from North America, Europe, Asia
        regions = [
            {"lat": 39.8, "lon": -98.5, "dist": 2000},  # USA
            {"lat": 50.0, "lon": 15.0, "dist": 2000},   # Europe
            {"lat": 35.0, "lon": 105.0, "dist": 2000},  # Asia / China
            {"lat": -25.0, "lon": 133.0, "dist": 2000}, # Australia
            {"lat": 0.0, "lon": 20.0, "dist": 2500},    # Africa
            {"lat": -15.0, "lon": -60.0, "dist": 2000}  # South America
        ]
        
        all_adsb_flights = []

        # Fetch all regions in parallel for ~5x speedup
        def _fetch_region(r):
            url = f"https://api.adsb.lol/v2/lat/{r['lat']}/lon/{r['lon']}/dist/{r['dist']}"
            try:
                res = fetch_with_curl(url, timeout=10)
                if res.status_code == 200:
                    data = res.json()
                    return data.get("ac", [])
            except Exception as e:
                logger.warning(f"Region fetch failed for lat={r['lat']}: {e}")
            return []

        # Fetch all regions in parallel for maximum speed
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            results = pool.map(_fetch_region, regions)
        for region_flights in results:
            all_adsb_flights.extend(region_flights)

        # ---------------------------------------------------------------------------
        # OpenSky Regional Fallback (Africa, Asia, South America)
        # ---------------------------------------------------------------------------
        now = time.time()
        global last_opensky_fetch, cached_opensky_flights
        
        # OpenSky has a 400 req/day limit (~16 pings/hour)
        # 5 minutes = 288 pings/day (Safe margin)
        if now - last_opensky_fetch > 300:
            token = opensky_client.get_token()
            if token:
                opensky_regions = [
                    {"name": "Africa", "bbox": {"lamin": -35.0, "lomin": -20.0, "lamax": 38.0, "lomax": 55.0}},
                    {"name": "Asia", "bbox": {"lamin": 0.0, "lomin": 30.0, "lamax": 75.0, "lomax": 150.0}},
                    {"name": "South America", "bbox": {"lamin": -60.0, "lomin": -95.0, "lamax": 15.0, "lomax": -30.0}}
                ]
                
                new_opensky_flights = []
                for os_reg in opensky_regions:
                    try:
                        bb = os_reg["bbox"]
                        os_url = f"https://opensky-network.org/api/states/all?lamin={bb['lamin']}&lomin={bb['lomin']}&lamax={bb['lamax']}&lomax={bb['lomax']}"
                        headers = {"Authorization": f"Bearer {token}"}
                        os_res = requests.get(os_url, headers=headers, timeout=15)
                        
                        if os_res.status_code == 200:
                            os_data = os_res.json()
                            states = os_data.get("states") or []
                            logger.info(f"OpenSky: Fetched {len(states)} states for {os_reg['name']}")
                            
                            for s in states:
                                # OpenSky state vector mapping:
                                # 0icao, 1callsign, 2country, 3time, 4last, 5lon, 6lat, 7baro, 8ground, 9vel, 10track, 11vert, 12sens, 13geo, 14sqk
                                new_opensky_flights.append({
                                    "hex": s[0],
                                    "flight": s[1].strip() if s[1] else "UNKNOWN",
                                    "r": s[2],
                                    "lon": s[5],
                                    "lat": s[6],
                                    "alt_baro": (s[7] * 3.28084) if s[7] else 0, # Meters to Feet for internal consistency
                                    "track": s[10] or 0,
                                    "gs": (s[9] * 1.94384) if s[9] else 0, # m/s to knots
                                    "t": "Unknown", # Model unknown in states API
                                    "is_opensky": True
                                })
                        else:
                            logger.warning(f"OpenSky API {os_reg['name']} failed: {os_res.status_code}")
                    except Exception as ex:
                        logger.error(f"OpenSky fetching error for {os_reg['name']}: {ex}")
                
                cached_opensky_flights = new_opensky_flights
                last_opensky_fetch = now
        
        # Merge cached OpenSky flights, but deduplicate by icao24 hex code
        # ADS-B Exchange is primary; OpenSky only fills gaps
        seen_hex = set()
        for f in all_adsb_flights:
            h = f.get("hex")
            if h:
                seen_hex.add(h.lower().strip())
        for osf in cached_opensky_flights:
            h = osf.get("hex")
            if h and h.lower().strip() not in seen_hex:
                all_adsb_flights.append(osf)
                seen_hex.add(h.lower().strip())

        # -------------------------------------------------------------------
        # Supplemental Sources: airplanes.live + adsb.fi (blind-spot gap-fill)
        # Only adds aircraft whose ICAO hex is NOT already in seen_hex.
        # -------------------------------------------------------------------
        try:
            gap_fill = _fetch_supplemental_sources(seen_hex)
            for f in gap_fill:
                all_adsb_flights.append(f)
                h = f.get("hex", "").lower().strip()
                if h:
                    seen_hex.add(h)
            if gap_fill:
                logger.info(f"Gap-fill: added {len(gap_fill)} aircraft to pipeline")
        except Exception as e:
            logger.warning(f"Supplemental source fetch failed (non-fatal): {e}")

        if all_adsb_flights:
            
            # The user requested maximum flight density. Rendering all available aircraft.
            sampled = all_adsb_flights
            
            # Spin up the background batch route resolver if it's not already trickling
            if not routes_fetch_in_progress:
                threading.Thread(target=fetch_routes_background, args=(sampled,), daemon=True).start()
            
            for f in sampled:
                try:
                    lat = f.get("lat")
                    lng = f.get("lon")
                    heading = f.get("track") or 0
                    
                    if lat is None or lng is None:
                        continue
                        
                    flight_str = str(f.get("flight", "UNKNOWN")).strip()
                    if not flight_str or flight_str == "UNKNOWN":
                        flight_str = str(f.get("hex", "Unknown"))
                        
                    # Origin and destination are fetched via the background thread and cached
                    origin_loc = None
                    dest_loc = None
                    origin_name = "UNKNOWN"
                    dest_name = "UNKNOWN"
                    
                    if flight_str in dynamic_routes_cache:
                        cached = dynamic_routes_cache[flight_str]
                        origin_name = cached["orig_name"]
                        dest_name = cached["dest_name"]
                        origin_loc = cached["orig_loc"]
                        dest_loc = cached["dest_loc"]
                    
                    # Extract 3-letter ICAO Airline Code from CallSign (e.g. UAL123 -> UAL)
                    airline_code = ""
                    match = _RE_AIRLINE_CODE_1.match(flight_str)
                    if not match:
                        match = _RE_AIRLINE_CODE_2.match(flight_str)
                    if match:
                        airline_code = match.group(1)

                    alt_raw = f.get("alt_baro")
                    alt_value = 0
                    if isinstance(alt_raw, (int, float)):
                        alt_value = alt_raw * 0.3048
                    
                    # Ground speed from ADS-B (in knots)
                    gs_knots = f.get("gs")
                    speed_knots = round(gs_knots, 1) if isinstance(gs_knots, (int, float)) else None

                    model_upper = f.get("t", "").upper()

                    # Skip fixed structures (towers, oil platforms) that broadcast ADS-B
                    if model_upper == "TWR":
                        continue

                    ac_category = "heli" if model_upper in _HELI_TYPES_BACKEND else "plane"

                    flights.append({
                        "callsign": flight_str,
                        "country": f.get("r", "N/A"),
                        "lng": float(lng),
                        "lat": float(lat),
                        "alt": alt_value,
                        "heading": heading,
                        "type": "flight",
                        "origin_loc": origin_loc,
                        "dest_loc": dest_loc,
                        "origin_name": origin_name,
                        "dest_name": dest_name,
                        "registration": f.get("r", "N/A"),
                        "model": f.get("t", "Unknown"),
                        "icao24": f.get("hex", ""),
                        "speed_knots": speed_knots,
                        "squawk": f.get("squawk", ""),
                        "airline_code": airline_code,
                        "aircraft_category": ac_category,
                        "nac_p": f.get("nac_p")  # Navigation accuracy — used for GPS jamming detection
                    })
                except Exception as loop_e:
                    logger.error(f"Flight interpolation error: {loop_e}")
                    continue
                
    except Exception as e:
        logger.error(f"Error fetching adsb.lol flights: {e}")
        
    # Private jet ICAO type designator codes (business jets wealthy individuals fly)
    PRIVATE_JET_TYPES = {
        # Gulfstream
        "G150", "G200", "G280", "GLEX", "G500", "G550", "G600", "G650", "G700",
        "GLF2", "GLF3", "GLF4", "GLF5", "GLF6", "GL5T", "GL7T", "GV", "GIV",
        # Bombardier
        "CL30", "CL35", "CL60", "BD70", "BD10", "GL5T", "GL7T",
        "CRJ1", "CRJ2",  # Challenger variants used privately
        # Cessna Citation
        "C25A", "C25B", "C25C", "C500", "C501", "C510", "C525", "C526",
        "C550", "C560", "C56X", "C680", "C68A", "C700", "C750",
        # Dassault Falcon
        "FA10", "FA20", "FA50", "FA7X", "FA8X", "F900", "F2TH", "ASTR",
        # Embraer Business Jets
        "E35L", "E545", "E550", "E55P", "LEGA",  # Praetor / Legacy
        "PH10",  # Phenom 100
        "PH30",  # Phenom 300
        # Learjet
        "LJ23", "LJ24", "LJ25", "LJ28", "LJ31", "LJ35", "LJ36",
        "LJ40", "LJ45", "LJ55", "LJ60", "LJ70", "LJ75",
        # Hawker / Beechcraft
        "H25A", "H25B", "H25C", "HA4T", "BE40", "PRM1",
        # Other business jets
        "HDJT",  # HondaJet
        "PC24",  # Pilatus PC-24
        "EA50",  # Eclipse 500
        "SF50",  # Cirrus Vision Jet
        "GALX",  # IAI Galaxy
    }
    
    commercial = []
    private_jets = []
    private_ga = []
    tracked = []
    
    
    for f in flights:
        # Enrich every flight with plane-alert data
        enrich_with_plane_alert(f)
        enrich_with_tracked_names(f)
        
        callsign = f.get('callsign', '').strip().upper()
        # Heuristic: standard airline callsigns are 3 letters + 1 to 4 digits (e.g., AFR7403, BAW12)
        is_commercial_format = bool(re.match(r'^[A-Z]{3}\d{1,4}[A-Z]{0,2}$', callsign))
        
        if f.get('alert_category'):
            # This is a tracked aircraft — pull it out into tracked list
            f['type'] = 'tracked_flight'
            tracked.append(f)
        elif f.get('airline_code') or is_commercial_format:
            f['type'] = 'commercial_flight'
            commercial.append(f)
        elif f.get('model', '').upper() in PRIVATE_JET_TYPES:
            f['type'] = 'private_jet'
            private_jets.append(f)
        else:
            f['type'] = 'private_ga'
            private_ga.append(f)
    
    # --- Smart merge: protect against partial API failures ---
    # If the new dataset has dramatically fewer flights than what we already have,
    # a region fetch probably failed — keep the old data to prevent planes vanishing.
    prev_commercial_count = len(latest_data.get('commercial_flights', []))
    prev_total = prev_commercial_count + len(latest_data.get('private_jets', [])) + len(latest_data.get('private_flights', []))
    new_total = len(commercial) + len(private_jets) + len(private_ga)

    if new_total == 0:
        logger.warning("No civilian flights found! Skipping overwrite to prevent clearing the map.")
    elif prev_total > 100 and new_total < prev_total * 0.5:
        # Dramatic drop (>50% loss) — a region probably failed, keep existing data
        logger.warning(f"Flight count dropped from {prev_total} to {new_total} (>50% loss). Keeping previous data to prevent flicker.")
    else:
        # Merge: deduplicate by icao24, prefer new data
        import time as _time
        _now = _time.time()

        def _merge_category(new_list, old_list, max_stale_s=120):
            """Merge new flights with old, keeping stale entries for up to max_stale_s."""
            by_icao = {}
            # Old entries first (will be overwritten by new)
            for f in old_list:
                icao = f.get('icao24', '')
                if icao:
                    f.setdefault('_seen_at', _now)
                    # Evict if stale for too long
                    if (_now - f.get('_seen_at', _now)) < max_stale_s:
                        by_icao[icao] = f
            # New entries overwrite old
            for f in new_list:
                icao = f.get('icao24', '')
                if icao:
                    f['_seen_at'] = _now
                    by_icao[icao] = f
                else:
                    by_icao[id(f)] = f  # no icao — keep as unique
            return list(by_icao.values())

        with _data_lock:
            latest_data['commercial_flights'] = _merge_category(commercial, latest_data.get('commercial_flights', []))
            latest_data['private_jets'] = _merge_category(private_jets, latest_data.get('private_jets', []))
            latest_data['private_flights'] = _merge_category(private_ga, latest_data.get('private_flights', []))

    _mark_fresh("commercial_flights", "private_jets", "private_flights")

    # Always write raw flights for GPS jamming analysis (nac_p field)
    if flights:
        latest_data['flights'] = flights
    
    # Merge tracked civilian flights with any tracked military flights
    # CRITICAL: Update positions for already-tracked aircraft on every cycle,
    # not just add new ones — otherwise tracked positions go stale.
    existing_tracked = latest_data.get('tracked_flights', [])
    
    # Build a map of fresh tracked data keyed by icao24
    fresh_tracked_map = {}
    for t in tracked:
        icao = t.get('icao24', '').upper()
        if icao:
            fresh_tracked_map[icao] = t
    
    # Update existing tracked entries with fresh positions, preserve metadata
    merged_tracked = []
    seen_icaos = set()
    for old_t in existing_tracked:
        icao = old_t.get('icao24', '').upper()
        if icao in fresh_tracked_map:
            # Fresh data available — use it, but preserve any extra metadata from old entry
            fresh = fresh_tracked_map[icao]
            for key in ('alert_category', 'alert_operator', 'alert_special', 'alert_flag'):
                if key in old_t and key not in fresh:
                    fresh[key] = old_t[key]
            merged_tracked.append(fresh)
            seen_icaos.add(icao)
        else:
            # No fresh data (military-only tracked, or plane landed/out of range)
            merged_tracked.append(old_t)
            seen_icaos.add(icao)
    
    # Add any newly-discovered tracked aircraft
    for icao, t in fresh_tracked_map.items():
        if icao not in seen_icaos:
            merged_tracked.append(t)
    
    latest_data['tracked_flights'] = merged_tracked
    logger.info(f"Tracked flights: {len(merged_tracked)} total ({len(fresh_tracked_map)} fresh from civilian)")
    
    # -----------------------------------------------------------------------
    # Flight Trail Accumulation — build position history for unrouted flights
    # -----------------------------------------------------------------------
    def _accumulate_trail(f, now_ts, check_route=True):
        """Accumulate trail points for a single flight. Returns 1 if trail updated, 0 otherwise."""
        hex_id = f.get('icao24', '').lower()
        if not hex_id:
            return 0, None
        if check_route and f.get('origin_name', 'UNKNOWN') != 'UNKNOWN':
            f['trail'] = []
            return 0, hex_id
        lat, lng, alt = f.get('lat'), f.get('lng'), f.get('alt', 0)
        if lat is None or lng is None:
            f['trail'] = flight_trails.get(hex_id, {}).get('points', [])
            return 0, hex_id
        point = [round(lat, 5), round(lng, 5), round(alt, 1), round(now_ts)]
        if hex_id not in flight_trails:
            flight_trails[hex_id] = {'points': [], 'last_seen': now_ts}
        trail_data = flight_trails[hex_id]
        if trail_data['points'] and trail_data['points'][-1][0] == point[0] and trail_data['points'][-1][1] == point[1]:
            trail_data['last_seen'] = now_ts
        else:
            trail_data['points'].append(point)
            trail_data['last_seen'] = now_ts
        if len(trail_data['points']) > 200:
            trail_data['points'] = trail_data['points'][-200:]
        f['trail'] = trail_data['points']
        return 1, hex_id

    now_ts = datetime.utcnow().timestamp()
    all_lists = [commercial, private_jets, private_ga, existing_tracked]
    seen_hexes = set()
    trail_count = 0
    with _trails_lock:
        for flist in all_lists:
            for f in flist:
                count, hex_id = _accumulate_trail(f, now_ts, check_route=True)
                trail_count += count
                if hex_id:
                    seen_hexes.add(hex_id)

        # Also process military flights (separate list)
        for mf in latest_data.get('military_flights', []):
            count, hex_id = _accumulate_trail(mf, now_ts, check_route=False)
            trail_count += count
            if hex_id:
                seen_hexes.add(hex_id)

        # Prune stale trails (10 min for non-tracked, 30 min for tracked)
        tracked_hexes = {t.get('icao24', '').lower() for t in latest_data.get('tracked_flights', [])}
        stale_keys = []
        for k, v in flight_trails.items():
            cutoff = now_ts - 1800 if k in tracked_hexes else now_ts - 600
            if v['last_seen'] < cutoff:
                stale_keys.append(k)
        for k in stale_keys:
            del flight_trails[k]

        # Enforce global cap — evict oldest trails first
        if len(flight_trails) > _MAX_TRACKED_TRAILS:
            sorted_keys = sorted(flight_trails.keys(), key=lambda k: flight_trails[k]['last_seen'])
            evict_count = len(flight_trails) - _MAX_TRACKED_TRAILS
            for k in sorted_keys[:evict_count]:
                del flight_trails[k]

    logger.info(f"Trail accumulation: {trail_count} active trails, {len(stale_keys)} pruned, {len(flight_trails)} total")

    # -----------------------------------------------------------------------
    # GPS / GNSS Jamming Detection — aggregate NACp from ADS-B transponders
    # NACp (Navigation Accuracy Category for Position):
    #   11 = full accuracy (<3m), 8 = good (<93m), <8 = degraded = potential jamming
    # We use a 1°×1° grid (~111km at equator) to aggregate interference zones.
    # -----------------------------------------------------------------------
    try:
        jamming_grid = {}  # "lat,lng" -> {"degraded": int, "total": int}
        raw_flights = latest_data.get('flights', [])
        for rf in raw_flights:
            rlat = rf.get('lat')
            rlng = rf.get('lng') or rf.get('lon')
            if rlat is None or rlng is None:
                continue
            nacp = rf.get('nac_p')
            if nacp is None:
                continue
            # Grid key: snap to 1-degree cells
            grid_key = f"{int(rlat)},{int(rlng)}"
            if grid_key not in jamming_grid:
                jamming_grid[grid_key] = {"degraded": 0, "total": 0}
            jamming_grid[grid_key]["total"] += 1
            if nacp < 8:
                jamming_grid[grid_key]["degraded"] += 1

        jamming_zones = []
        for gk, counts in jamming_grid.items():
            if counts["total"] < 3:
                continue  # Need at least 3 aircraft to be meaningful
            ratio = counts["degraded"] / counts["total"]
            if ratio > 0.25:  # >25% degraded = jamming
                lat_i, lng_i = gk.split(",")
                severity = "low" if ratio < 0.5 else "medium" if ratio < 0.75 else "high"
                jamming_zones.append({
                    "lat": int(lat_i) + 0.5,  # Center of cell
                    "lng": int(lng_i) + 0.5,
                    "severity": severity,
                    "ratio": round(ratio, 2),
                    "degraded": counts["degraded"],
                    "total": counts["total"]
                })
        latest_data['gps_jamming'] = jamming_zones
        if jamming_zones:
            logger.info(f"GPS Jamming: {len(jamming_zones)} interference zones detected")
    except Exception as e:
        logger.error(f"GPS Jamming detection error: {e}")
        latest_data['gps_jamming'] = []

    # -----------------------------------------------------------------------
    # Holding Pattern Detection — flag aircraft circling in place
    # If cumulative heading change over last 8 trail points > 300°, it's circling
    # -----------------------------------------------------------------------
    try:
        holding_count = 0
        all_flight_lists = [commercial, private_jets, private_ga,
                            latest_data.get('tracked_flights', []),
                            latest_data.get('military_flights', [])]
        for flist in all_flight_lists:
            for f in flist:
                hex_id = f.get('icao24', '').lower()
                trail = flight_trails.get(hex_id, {}).get('points', [])
                if len(trail) < 6:
                    f['holding'] = False
                    continue
                # Calculate cumulative bearing change over last 8 points
                pts = trail[-8:]
                total_turn = 0.0
                prev_bearing = 0.0
                for i in range(1, len(pts)):
                    lat1, lng1 = math.radians(pts[i-1][0]), math.radians(pts[i-1][1])
                    lat2, lng2 = math.radians(pts[i][0]), math.radians(pts[i][1])
                    dlng = lng2 - lng1
                    x = math.sin(dlng) * math.cos(lat2)
                    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlng)
                    bearing = math.degrees(math.atan2(x, y)) % 360
                    if i > 1:
                        delta = abs(bearing - prev_bearing)
                        if delta > 180:
                            delta = 360 - delta
                        total_turn += delta
                    prev_bearing = bearing
                f['holding'] = total_turn > 300  # > 300° = nearly a full circle
                if f['holding']:
                    holding_count += 1
        if holding_count:
            logger.info(f"Holding patterns: {holding_count} aircraft circling")
    except Exception as e:
        logger.error(f"Holding pattern detection error: {e}")

    # Update timestamp so the ETag in /api/live-data/fast changes on every fetch cycle
    latest_data['last_updated'] = datetime.utcnow().isoformat()


def fetch_military_flights():
    # True ADS-B Exchange military data requires paid API access.
    # We will use adsb.lol (an open source ADSB aggregator) /v2/mil fallback.
    military_flights = []
    detected_uavs = []
    try:
        url = "https://api.adsb.lol/v2/mil"
        response = fetch_with_curl(url, timeout=10)
        if response.status_code == 200:
            ac = response.json().get('ac', [])
            for f in ac:
                try:
                    lat = f.get("lat")
                    lng = f.get("lon")
                    heading = f.get("track") or 0

                    if lat is None or lng is None:
                        continue

                    model = str(f.get("t", "UNKNOWN")).upper()
                    callsign = str(f.get("flight", "MIL-UNKN")).strip()

                    # Skip fixed structures (towers, oil platforms) that broadcast ADS-B
                    if model == "TWR":
                        continue

                    alt_raw = f.get("alt_baro")
                    alt_value = 0
                    if isinstance(alt_raw, (int, float)):
                        alt_value = alt_raw * 0.3048

                    # Ground speed from ADS-B (in knots)
                    gs_knots = f.get("gs")
                    speed_knots = round(gs_knots, 1) if isinstance(gs_knots, (int, float)) else None

                    # Check if this is a UAV/drone before classifying as regular military
                    is_uav, uav_type, wiki_url = _classify_uav(model, callsign)
                    if is_uav:
                        detected_uavs.append({
                            "id": f"uav-{f.get('hex', '')}",
                            "callsign": callsign,
                            "aircraft_model": f.get("t", "Unknown"),
                            "lat": float(lat),
                            "lng": float(lng),
                            "alt": alt_value,
                            "heading": heading,
                            "speed_knots": speed_knots,
                            "country": f.get("r", "Unknown"),
                            "uav_type": uav_type,
                            "wiki": wiki_url or "",
                            "type": "uav",
                            "registration": f.get("r", "N/A"),
                            "icao24": f.get("hex", ""),
                            "squawk": f.get("squawk", ""),
                        })
                        continue  # Don't double-count as military flight

                    mil_cat = "default"
                    if "H" in model and any(c.isdigit() for c in model):
                        mil_cat = "heli"
                    elif any(k in model for k in ["K35", "K46", "A33"]):
                        mil_cat = "tanker"
                    elif any(k in model for k in ["F16", "F35", "F22", "F15", "F18", "T38", "T6", "A10"]):
                        mil_cat = "fighter"
                    elif any(k in model for k in ["C17", "C5", "C130", "C30", "A400", "V22"]):
                        mil_cat = "cargo"
                    elif any(k in model for k in ["P8", "E3", "E8", "U2"]):
                        mil_cat = "recon"

                    military_flights.append({
                        "callsign": callsign,
                        "country": f.get("r", "Military Asset"),
                        "lng": float(lng),
                        "lat": float(lat),
                        "alt": alt_value,
                        "heading": heading,
                        "type": "military_flight",
                        "military_type": mil_cat,
                        "origin_loc": None,
                        "dest_loc": None,
                        "origin_name": "UNKNOWN",
                        "dest_name": "UNKNOWN",
                        "registration": f.get("r", "N/A"),
                        "model": f.get("t", "Unknown"),
                        "icao24": f.get("hex", ""),
                        "speed_knots": speed_knots,
                        "squawk": f.get("squawk", "")
                    })
                except Exception as loop_e:
                    logger.error(f"Mil flight interpolation error: {loop_e}")
                    continue
    except Exception as e:
        logger.error(f"Error fetching military flights: {e}")

    if not military_flights and not detected_uavs:
        # API failed or rate limited — log but do NOT inject fake data
        logger.warning("No military flights retrieved — keeping previous data if available")
        # Preserve existing data rather than overwriting with empty
        if latest_data.get('military_flights'):
            return

    latest_data['military_flights'] = military_flights
    latest_data['uavs'] = detected_uavs
    _mark_fresh("military_flights", "uavs")
    logger.info(f"UAVs: {len(detected_uavs)} real drones detected via ADS-B")
    
    # Cross-reference military flights with Plane-Alert DB
    tracked_mil = []
    remaining_mil = []
    for mf in military_flights:
        enrich_with_plane_alert(mf)
        if mf.get('alert_category'):
            mf['type'] = 'tracked_flight'
            tracked_mil.append(mf)
        else:
            remaining_mil.append(mf)
    latest_data['military_flights'] = remaining_mil
    
    # Store tracked military flights — update positions for existing entries
    existing_tracked = latest_data.get('tracked_flights', [])
    fresh_mil_map = {}
    for t in tracked_mil:
        icao = t.get('icao24', '').upper()
        if icao:
            fresh_mil_map[icao] = t
    
    # Update existing military tracked entries with fresh positions
    updated_tracked = []
    seen_icaos = set()
    for old_t in existing_tracked:
        icao = old_t.get('icao24', '').upper()
        if icao in fresh_mil_map:
            fresh = fresh_mil_map[icao]
            for key in ('alert_category', 'alert_operator', 'alert_special', 'alert_flag'):
                if key in old_t and key not in fresh:
                    fresh[key] = old_t[key]
            updated_tracked.append(fresh)
            seen_icaos.add(icao)
        else:
            updated_tracked.append(old_t)
            seen_icaos.add(icao)
    for icao, t in fresh_mil_map.items():
        if icao not in seen_icaos:
            updated_tracked.append(t)
    latest_data['tracked_flights'] = updated_tracked
    logger.info(f"Tracked flights: {len(updated_tracked)} total ({len(tracked_mil)} from military)")








def fetch_traffic():
    # Deprecated: TomTom warning signs removed from UI to declutter CCTV mesh
    latest_data["traffic"] = []


# ---------------------------------------------------------------------------
# Real UAV detection from ADS-B data — filters military drone transponders
# ---------------------------------------------------------------------------
_UAV_TYPE_CODES = {"Q9", "R4", "TB2", "MALE", "HALE", "HERM", "HRON"}
_UAV_CALLSIGN_PREFIXES = ("FORTE", "GHAWK", "REAP", "BAMS", "UAV", "UAS")
_UAV_MODEL_KEYWORDS = ("RQ-", "MQ-", "RQ4", "MQ9", "MQ4", "MQ1", "REAPER", "GLOBALHAWK", "TRITON", "PREDATOR", "HERMES", "HERON", "BAYRAKTAR")
_UAV_WIKI = {
    "RQ4": "https://en.wikipedia.org/wiki/Northrop_Grumman_RQ-4_Global_Hawk",
    "RQ-4": "https://en.wikipedia.org/wiki/Northrop_Grumman_RQ-4_Global_Hawk",
    "MQ4": "https://en.wikipedia.org/wiki/Northrop_Grumman_MQ-4C_Triton",
    "MQ-4": "https://en.wikipedia.org/wiki/Northrop_Grumman_MQ-4C_Triton",
    "MQ9": "https://en.wikipedia.org/wiki/General_Atomics_MQ-9_Reaper",
    "MQ-9": "https://en.wikipedia.org/wiki/General_Atomics_MQ-9_Reaper",
    "MQ1": "https://en.wikipedia.org/wiki/General_Atomics_MQ-1C_Gray_Eagle",
    "MQ-1": "https://en.wikipedia.org/wiki/General_Atomics_MQ-1C_Gray_Eagle",
    "REAPER": "https://en.wikipedia.org/wiki/General_Atomics_MQ-9_Reaper",
    "GLOBALHAWK": "https://en.wikipedia.org/wiki/Northrop_Grumman_RQ-4_Global_Hawk",
    "TRITON": "https://en.wikipedia.org/wiki/Northrop_Grumman_MQ-4C_Triton",
    "PREDATOR": "https://en.wikipedia.org/wiki/General_Atomics_MQ-1_Predator",
    "HERMES": "https://en.wikipedia.org/wiki/Elbit_Hermes_900",
    "HERON": "https://en.wikipedia.org/wiki/IAI_Heron",
    "BAYRAKTAR": "https://en.wikipedia.org/wiki/Bayraktar_TB2",
}

def _classify_uav(model: str, callsign: str):
    """Check if an aircraft is a UAV based on type code, callsign prefix, or model keywords.
    Returns (is_uav, uav_type, wiki_url) or (False, None, None)."""
    model_up = model.upper().replace(" ", "")
    callsign_up = callsign.upper().strip()

    # Check ICAO type codes
    if model_up in _UAV_TYPE_CODES:
        uav_type = "HALE Surveillance" if model_up in ("R4", "HALE") else "MALE ISR"
        wiki = _UAV_WIKI.get(model_up, "")
        return True, uav_type, wiki

    # Check callsign prefixes (must also have a military-ish model)
    for prefix in _UAV_CALLSIGN_PREFIXES:
        if callsign_up.startswith(prefix):
            uav_type = "HALE Surveillance" if prefix in ("FORTE", "GHAWK", "BAMS") else "MALE ISR"
            wiki = _UAV_WIKI.get(prefix, "")
            if prefix == "FORTE":
                wiki = _UAV_WIKI["RQ4"]
            elif prefix == "BAMS":
                wiki = _UAV_WIKI["MQ4"]
            return True, uav_type, wiki

    # Check model keywords
    for kw in _UAV_MODEL_KEYWORDS:
        if kw in model_up:
            # Determine type from keyword
            if any(h in model_up for h in ("RQ4", "RQ-4", "GLOBALHAWK")):
                return True, "HALE Surveillance", _UAV_WIKI.get(kw, "")
            elif any(h in model_up for h in ("MQ4", "MQ-4", "TRITON")):
                return True, "HALE Maritime Surveillance", _UAV_WIKI.get(kw, "")
            elif any(h in model_up for h in ("MQ9", "MQ-9", "REAPER")):
                return True, "MALE Strike/ISR", _UAV_WIKI.get(kw, "")
            elif any(h in model_up for h in ("MQ1", "MQ-1", "PREDATOR")):
                return True, "MALE ISR/Strike", _UAV_WIKI.get(kw, "")
            elif "BAYRAKTAR" in model_up or "TB2" in model_up:
                return True, "MALE Strike", _UAV_WIKI.get("BAYRAKTAR", "")
            elif "HERMES" in model_up:
                return True, "MALE ISR", _UAV_WIKI.get("HERMES", "")
            elif "HERON" in model_up:
                return True, "MALE ISR", _UAV_WIKI.get("HERON", "")
            return True, "MALE ISR", _UAV_WIKI.get(kw, "")

    return False, None, None

cached_airports = []
flight_trails = {}  # {icao_hex: {points: [[lat, lng, alt, ts], ...], last_seen: ts}}
_trails_lock = threading.Lock()
_MAX_TRACKED_TRAILS = 2000  # Global cap on number of aircraft trails in memory

# (math imported at module top)

def update_fast_data():
    """Fast-tier: moving entities that need frequent updates (every 60s)."""
    logger.info("Fast-tier data update starting...")
    fast_funcs = [
        fetch_flights,
        fetch_military_flights,  # Also detects UAVs from ADS-B
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(fast_funcs)) as executor:
        futures = [executor.submit(func) for func in fast_funcs]
        concurrent.futures.wait(futures)
    # Run any fast-tier module-loader modules (no-op until modules are enabled)
    loader.run_fast_modules()
    with _data_lock:
        latest_data['last_updated'] = datetime.utcnow().isoformat()
    logger.info("Fast-tier update complete.")

def update_slow_data():
    """Slow-tier: feeds that change infrequently (every 30min)."""
    logger.info("Slow-tier data update starting...")
    slow_funcs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(slow_funcs)) as executor:
        futures = [executor.submit(func) for func in slow_funcs]
        concurrent.futures.wait(futures)
    # Run any slow-tier module-loader modules (no-op until modules are enabled)
    loader.run_slow_modules()
    logger.info("Slow-tier update complete.")

def update_all_data():
    """Full update — runs on startup. Fast and slow tiers run IN PARALLEL for fastest startup."""
    logger.info("Full data update starting (parallel)...")
    # Run fast + slow in parallel so the user sees data ASAP
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(update_fast_data)
        f2 = pool.submit(update_slow_data)
        concurrent.futures.wait([f1, f2])
    logger.info("Full data update complete.")

scheduler = BackgroundScheduler()

def start_scheduler():
    init_db()
    
    # Run full update once on startup
    scheduler.add_job(update_all_data, 'date', run_date=datetime.now())
    
    # Fast tier: every 60 seconds (flights, ships, military+UAVs, satellites)
    scheduler.add_job(update_fast_data, 'interval', seconds=60)
    
    # Slow tier: every 30 minutes (news, stocks, weather, geopolitics)
    scheduler.add_job(update_slow_data, 'interval', minutes=30)
    
    # Start any module-loader modules (no-op until modules.yaml entries are enabled)
    loader.start_all(None, latest_data, _data_lock, _mark_fresh, scheduler)

    scheduler.start()

def stop_scheduler():
    loader.stop_all()
    scheduler.shutdown()

def get_latest_data():
    with _data_lock:
        return dict(latest_data)

