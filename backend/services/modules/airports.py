import csv
import io
import logging

from services.network_utils import fetch_with_curl

logger = logging.getLogger(__name__)

KEY = "airports"
TIER = "slow"
RUN_ONCE = True
DEFAULT = []


def fetch():
    # Import the shared cache that find_nearest_airport() reads from data_fetcher
    import services.data_fetcher as df
    if df.cached_airports:
        logger.info(f"Airports: using cached {len(df.cached_airports)} airports")
        return list(df.cached_airports)

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
        # Populate the shared cache so find_nearest_airport() works immediately
        df.cached_airports.clear()
        df.cached_airports.extend(airports)

    return airports if airports else None
