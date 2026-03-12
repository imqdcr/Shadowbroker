import logging
from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

KEY = "earthquakes"
TIER = "slow"
DEFAULT = []


def fetch():
    quakes = []
    try:
        url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
        response = fetch_with_curl(url, timeout=10)
        if response.status_code == 200:
            features = response.json().get("features", [])
            for f in features[:50]:
                mag = f["properties"]["mag"]
                lng, lat, depth = f["geometry"]["coordinates"]
                quakes.append({
                    "id": f["id"],
                    "mag": mag,
                    "lat": lat,
                    "lng": lng,
                    "place": f["properties"]["place"]
                })
    except Exception as e:
        logger.error(f"Error fetching earthquakes: {e}")
    return quakes if quakes else None
