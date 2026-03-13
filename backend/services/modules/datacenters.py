import json
import logging
import time
from pathlib import Path

from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

KEY = "datacenters"
TIER = "slow"
DEFAULT = []

_DC_CACHE_PATH = Path(__file__).parent.parent.parent / "data" / "datacenters.json"
_DC_URL = "https://raw.githubusercontent.com/Ringmast4r/Data-Center-Map---Global/1f290297c6a11454dc7a47bf95aef7cf0fe1d34c/datacenters_cleaned.json"

_COUNTRY_BBOX: dict[str, tuple[float, float, float, float]] = {
    "Argentina": (-55, -21, -74, -53), "Australia": (-44, -10, 112, 154),
    "Bolivia": (-23, -9, -70, -57), "Brazil": (-34, 6, -74, -34),
    "Chile": (-56, -17, -76, -66), "Colombia": (-5, 13, -82, -66),
    "Ecuador": (-5, 2, -81, -75), "Indonesia": (-11, 6, 95, 141),
    "Kenya": (-5, 5, 34, 42), "Madagascar": (-26, -12, 43, 51),
    "Mozambique": (-27, -10, 30, 41), "New Zealand": (-47, -34, 166, 179),
    "Paraguay": (-28, -19, -63, -54), "Peru": (-18, 0, -82, -68),
    "South Africa": (-35, -22, 16, 33), "Tanzania": (-12, -1, 29, 41),
    "Uruguay": (-35, -30, -59, -53), "Zimbabwe": (-23, -15, 25, 34),
    "United States": (24, 72, -180, -65), "Canada": (41, 84, -141, -52),
    "United Kingdom": (49, 61, -9, 2), "Germany": (47, 55, 5, 16),
    "France": (41, 51, -5, 10), "Japan": (24, 46, 123, 146),
    "India": (6, 36, 68, 98), "China": (18, 54, 73, 135),
    "Singapore": (1, 2, 103, 105), "Spain": (36, 44, -10, 5),
    "Netherlands": (50, 54, 3, 8), "Sweden": (55, 70, 11, 25),
    "Italy": (36, 47, 6, 19), "Russia": (41, 82, 19, 180),
    "Mexico": (14, 33, -118, -86), "Nigeria": (4, 14, 2, 15),
    "Thailand": (5, 21, 97, 106), "Malaysia": (0, 8, 99, 120),
    "Philippines": (4, 21, 116, 127), "South Korea": (33, 39, 124, 132),
    "Taiwan": (21, 26, 119, 123), "Hong Kong": (22, 23, 113, 115),
    "Vietnam": (8, 24, 102, 110), "Poland": (49, 55, 14, 25),
    "Switzerland": (45, 48, 5, 11), "Austria": (46, 49, 9, 17),
    "Belgium": (49, 52, 2, 7), "Denmark": (54, 58, 8, 16),
    "Finland": (59, 70, 20, 32), "Norway": (57, 72, 4, 32),
    "Ireland": (51, 56, -11, -5), "Portugal": (36, 42, -10, -6),
    "Turkey": (35, 42, 25, 45), "Israel": (29, 34, 34, 36),
    "UAE": (22, 27, 51, 56), "Saudi Arabia": (16, 33, 34, 56),
}

_SOUTHERN_COUNTRIES = {
    "Argentina", "Australia", "Bolivia", "Brazil", "Chile", "Madagascar",
    "Mozambique", "New Zealand", "Paraguay", "Peru", "South Africa",
    "Tanzania", "Uruguay", "Zimbabwe",
}


def _fix_dc_coords(lat: float, lng: float, country: str) -> tuple[float, float] | None:
    if country in _SOUTHERN_COUNTRIES and lat > 0:
        lat = -lat
    bbox = _COUNTRY_BBOX.get(country)
    if bbox:
        lat_min, lat_max, lng_min, lng_max = bbox
        if lat_min <= lat <= lat_max and lng_min <= lng <= lng_max:
            return lat, lng
        if lat_min <= -lat <= lat_max and lng_min <= lng <= lng_max:
            return -lat, lng
        return None
    return lat, lng


def fetch():
    dcs = []
    try:
        raw = None
        if _DC_CACHE_PATH.exists():
            age_days = (time.time() - _DC_CACHE_PATH.stat().st_mtime) / 86400
            if age_days < 7:
                raw = json.loads(_DC_CACHE_PATH.read_text(encoding="utf-8"))
        if raw is None:
            resp = fetch_with_curl(_DC_URL, timeout=20)
            if resp.status_code == 200:
                raw = resp.json()
                _DC_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
                _DC_CACHE_PATH.write_text(json.dumps(raw), encoding="utf-8")
        if raw:
            dropped = 0
            for entry in raw:
                coords = entry.get("city_coords")
                if not coords or not isinstance(coords, list) or len(coords) < 2:
                    continue
                lat, lng = coords[0], coords[1]
                if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                    continue
                country = entry.get("country", "")
                fixed = _fix_dc_coords(lat, lng, country)
                if fixed is None:
                    dropped += 1
                    continue
                lat, lng = fixed
                dcs.append({
                    "name": entry.get("name", "Unknown"),
                    "company": entry.get("company", ""),
                    "city": entry.get("city", ""),
                    "country": country,
                    "lat": lat,
                    "lng": lng,
                })
            if dropped:
                logger.info(f"Data centers: dropped {dropped} entries with mismatched coordinates")
        logger.info(f"Data centers: {len(dcs)} with valid coordinates")
    except Exception as e:
        logger.error(f"Error fetching data centers: {e}")
    return dcs if dcs else None