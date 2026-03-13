import csv
import io
import logging

from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

KEY = "airports"
TIER = "slow"
RUN_ONCE = True
DEFAULT = []
ENABLED = True

# Module-level cache — shared with anyone who imports from here
cached_airports: list = []


def fetch():
    global cached_airports
    if cached_airports:
        logger.info(f"Airports: using cached {len(cached_airports)} airports")
        return list(cached_airports)

    logger.info("Downloading global airports database from ourairports.com...")
    airports = []
    try:
        url = "https://ourairports.com/data/airports.csv"
        response = fetch_with_curl(url, timeout=15)
        if response.status_code == 200:
            reader = csv.DictReader(io.StringIO(response.text))
            for row in reader:
                if row["type"] == "large_airport" and row["iata_code"]:
                    airports.append({
                        "id": row["ident"],
                        "name": row["name"],
                        "iata": row["iata_code"],
                        "lat": float(row["latitude_deg"]),
                        "lng": float(row["longitude_deg"]),
                        "type": "airport",
                    })
            logger.info(f"Airports: loaded {len(airports)} large airports")
    except Exception as e:
        logger.error(f"Error fetching airports: {e}")

    if airports:
        cached_airports.clear()
        cached_airports.extend(airports)

    return airports if airports else None


def find_nearest_airport(lat: float, lng: float) -> dict | None:
    """Find the nearest large airport to the given coordinates."""
    if not cached_airports:
        return None
    nearest = None
    min_dist = float("inf")
    for apt in cached_airports:
        dist = (apt["lat"] - lat) ** 2 + (apt["lng"] - lng) ** 2
        if dist < min_dist:
            min_dist = dist
            nearest = apt
    return nearest