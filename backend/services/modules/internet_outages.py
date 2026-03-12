import heapq
import logging
import time
import urllib.parse

from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

KEY = "internet_outages"
TIER = "slow"
DEFAULT = []

RELIABLE_DATASOURCES = {"bgp", "ping-slash24"}

_region_geocode_cache: dict = {}


def _geocode_region(region_name: str, country_name: str) -> tuple | None:
    cache_key = f"{region_name}|{country_name}"
    if cache_key in _region_geocode_cache:
        return _region_geocode_cache[cache_key]
    try:
        query = urllib.parse.quote(f"{region_name}, {country_name}")
        url = f"https://nominatim.openstreetmap.org/search?q={query}&format=json&limit=1"
        response = fetch_with_curl(url, timeout=8, headers={"User-Agent": "ShadowBroker-OSINT/1.0"})
        if response.status_code == 200:
            results = response.json()
            if results:
                lat = float(results[0]["lat"])
                lon = float(results[0]["lon"])
                _region_geocode_cache[cache_key] = (lat, lon)
                return (lat, lon)
    except Exception:
        pass
    _region_geocode_cache[cache_key] = None
    return None


def fetch():
    outages = []
    try:
        now = int(time.time())
        start = now - 86400
        url = f"https://api.ioda.inetintel.cc.gatech.edu/v2/outages/alerts?from={start}&until={now}&limit=500"
        response = fetch_with_curl(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            alerts = data.get("data", [])
            region_outages = {}
            for alert in alerts:
                entity = alert.get("entity", {})
                etype = entity.get("type", "")
                level = alert.get("level", "")
                if level == "normal" or etype != "region":
                    continue
                datasource = alert.get("datasource", "")
                if datasource not in RELIABLE_DATASOURCES:
                    continue
                code = entity.get("code", "")
                name = entity.get("name", "")
                attrs = entity.get("attrs", {})
                country_code = attrs.get("country_code", "")
                country_name = attrs.get("country_name", "")
                value = alert.get("value", 0)
                history_value = alert.get("historyValue", 0)
                severity = 0
                if history_value and history_value > 0:
                    severity = round((1 - value / history_value) * 100)
                severity = max(0, min(severity, 100))
                if severity < 10:
                    continue
                if code not in region_outages or severity > region_outages[code]["severity"]:
                    region_outages[code] = {
                        "region_code": code,
                        "region_name": name,
                        "country_code": country_code,
                        "country_name": country_name,
                        "level": level,
                        "datasource": datasource,
                        "severity": severity,
                    }
            geocoded = []
            for rcode, r in region_outages.items():
                coords = _geocode_region(r["region_name"], r["country_name"])
                if coords:
                    r["lat"] = coords[0]
                    r["lng"] = coords[1]
                    geocoded.append(r)
            outages = heapq.nlargest(100, geocoded, key=lambda x: x["severity"])
        logger.info(f"Internet outages: {len(outages)} regions affected")
    except Exception as e:
        logger.error(f"Error fetching internet outages: {e}")
    return outages if outages else None
